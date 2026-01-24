import { PDFDocument, StandardFonts, rgb } from "pdf-lib";

/**
 * FULL Worker: CSV import + continue + dashboard + rebuild + mail + validate
 *
 * Added:
 *  POST /api/admin/receipt/import/validate   (CSV事前チェック)
 *
 * Existing:
 *  POST /api/admin/receipt/import
 *  POST /api/admin/receipt/import/continue
 *  GET  /api/admin/receipt/dashboard
 *  POST /api/admin/receipt/rebuild
 *  POST /api/admin/receipt/email/send-one
 *  POST /api/admin/receipt/email/send-bulk
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const host = url.hostname;
    const path = url.pathname;

    try {
      // =============== MEMBER UI ===============
      if (host === "kamikumite.worlddivinelight.org" && path.startsWith("/receipt")) {
        return html(memberPortalHtml(), 200);
      }

      // =============== MEMBER API (OTP + pdf) ===============
      if (host === "api.kamikumite.worlddivinelight.org" && path.startsWith("/api/receipt/")) {
        const allow = env.PORTAL_ORIGIN || "https://kamikumite.worlddivinelight.org";
        const origin = request.headers.get("Origin") || "";
        if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors(allow, origin) });

        if (path === "/api/receipt/request-code" && request.method === "POST") {
          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allow, origin);

          const hs = await hubspotGetContactByEmail(env, email, ["member_id","receipt_portal_eligible","receipt_years_available"]);
          if (hs.status === 404) return jsonC({ ok: false, error: "not_registered" }, 404, allow, origin);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow, origin);

          const p = (await hs.json()).properties || {};
          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allow, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(String(p.receipt_years_available || "").trim());
          if (!memberId || !years.length) return jsonC({ ok:false, error:"not_ready" }, 403, allow, origin);

          const code = String(Math.floor(100000 + Math.random() * 900000));
          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
          const hash = await sha256Hex(`${code}:${secret}`);

          await env.OTP_KV.put(`otp:${email}`, JSON.stringify({
            hash, member_id: memberId, years, exp: Date.now()+600000, attempts: 0
          }), { expirationTtl: 600 });

          await sendResend(env, { to: email, subject: "Your verification code", html: otpHtml(code) });
          return jsonC({ ok: true }, 200, allow, origin);
        }

        if (path === "/api/receipt/verify-code" && request.method === "POST") {
          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          const code = String(body?.code || "").trim();
          const rec = await env.OTP_KV.get(`otp:${email}`, "json");
          if (!rec) return jsonC({ ok: false, error: "expired" }, 401, allow, origin);

          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
          const hash = await sha256Hex(`${code}:${secret}`);
          if (hash !== rec.hash) return jsonC({ ok: false, error: "wrong" }, 401, allow, origin);

          await env.OTP_KV.delete(`otp:${email}`);

          const session = makeSession(env, { email, member_id: rec.member_id });
          const cookie = `receipt_session=${session}; Path=/; Secure; HttpOnly; SameSite=Lax; Domain=.worlddivinelight.org; Max-Age=${7*86400}`;

          return new Response(JSON.stringify({ ok: true, years: rec.years || [] }), {
            status: 200,
            headers: { ...cors(allow, origin), "content-type":"application/json", "set-cookie": cookie }
          });
        }

        if (path === "/api/receipt/pdf" && request.method === "GET") {
          const s = verifySession(env, readCookie(request.headers.get("Cookie")||"", "receipt_session"));
          if (!s.ok) return jsonC({ ok:false }, 401, allow, origin);

          const year = String(url.searchParams.get("year") || "").trim();
          const hs = await hubspotGetContactByEmail(env, s.email, ["member_id","receipt_years_available","receipt_portal_eligible"]);
          if (!hs.ok) return jsonC({ ok:false, error:"hubspot_get_failed" }, 500, allow, origin);

          const p = (await hs.json()).properties || {};
          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok:false }, 403, allow, origin);
          if (!parseYears(p.receipt_years_available).includes(year)) return jsonC({ ok:false }, 403, allow, origin);

          const key = `receipts/${p.member_id}/${year}.pdf`;
          const obj = await env.RECEIPTS_BUCKET.get(key);
          if (!obj) return jsonC({ ok:false, error:"pdf_not_found" }, 404, allow, origin);

          return new Response(obj.body, { status:200, headers:{ ...cors(allow, origin), "content-type":"application/pdf" }});
        }

        return jsonC({ ok:false, error:"not_found" }, 404, allow, origin);
      }

      // =============== ADMIN API ===============
      if (!path.startsWith("/api/admin/receipt/")) return json({ ok:false, error:"not_found" }, 404);

      if (path === "/api/admin/receipt/_version") {
        return json({ ok:true, worker:"kamikumite-receipt", build:"CSV_IMPORT_v1+PDFGEN_v1+MAIL_v1+REBUILD_v1+VALIDATE_v1" });
      }

      // -------- VALIDATE (NEW) --------
      // POST /api/admin/receipt/import/validate (text/csv)
      if (path === "/api/admin/receipt/import/validate" && request.method === "POST") {
        const csvText = await request.text();
        const rows = parseCsv(csvText);
        if (!rows.length) return json({ ok:false, error:"no_rows" }, 400);

        const errors = [];
        const maxCheck = 500; // 安全のため上限（必要なら後で増やせます）
        const checkRows = rows.slice(0, maxCheck);

        for (let i=0; i<checkRows.length; i++) {
          const r = checkRows[i];
          const rowNo = i + 2; // headerが1行なのでデータは2行目から
          const member_id = String(r.member_id||"").trim();
          const branch = String(r.branch||"").trim();
          const cents = toCents(r.amount);
          const year = normYear(r.year);

          if (!member_id) { errors.push({ type:"MISSING_MEMBER_ID", row: rowNo }); continue; }
          if (!branch) { errors.push({ type:"MISSING_BRANCH", row: rowNo, member_id }); continue; }
          if (!Number.isFinite(cents)) { errors.push({ type:"INVALID_AMOUNT", row: rowNo, member_id }); continue; }
          if (!year) { errors.push({ type:"INVALID_YEAR", row: rowNo, member_id }); continue; }

          // HubSpot existence + email check
          const hs = await hubspotGetContactByIdProperty(env, member_id, "member_id", ["email","firstname","lastname"]);
          if (hs.status === 404 || !hs.ok) {
            errors.push({ type:"HUBSPOT_NOT_FOUND", row: rowNo, member_id });
            continue;
          }
          const p = (await hs.json()).properties || {};
          const email = normEmail(p.email);
          if (!email) {
            errors.push({ type:"MISSING_EMAIL", row: rowNo, member_id, name: fmtName(p.firstname, p.lastname, p.email) });
            continue;
          }
        }

        if (errors.length) {
          return json({ ok:false, errors, checked_rows: checkRows.length, total_rows: rows.length });
        }
        return json({ ok:true, checked_rows: checkRows.length, total_rows: rows.length });
      }

      // -------- DASHBOARD --------
      if (path === "/api/admin/receipt/dashboard" && request.method === "GET") {
        const year = normYear(url.searchParams.get("year")) ?? (new Date().getFullYear()-1);
        const rows = await env.RECEIPTS_DB.prepare(`
          SELECT year, member_id, branch, name, amount_cents, issue_date, pdf_key, status, error
          FROM receipt_annual
          WHERE year=?
          ORDER BY branch, name
        `).bind(year).all();
        return json({ ok:true, year, rows: rows.results || [] });
      }

      // -------- IMPORT --------
      if (path === "/api/admin/receipt/import" && request.method === "POST") {
        const csvText = await request.text();
        const rows = parseCsv(csvText);
        if (!rows.length) return json({ ok:false, error:"no_rows" }, 400);

        const job_id = crypto.randomUUID();
        const year = normYear(rows[0].year) ?? (new Date().getFullYear()-1);

        await env.RECEIPTS_DB.prepare(`
          INSERT INTO receipt_import_job(job_id,year,total_rows,processed_rows,ok_rows,ng_rows,next_index,status)
          VALUES(?,?,?,0,0,0,0,'READY')
        `).bind(job_id, year, rows.length).run();

        await env.RECEIPTS_BUCKET.put(`uploads/${job_id}.csv`, csvText, { httpMetadata:{ contentType:"text/csv" }});
        return json({ ok:true, job_id, year, total_rows: rows.length });
      }

      if (path === "/api/admin/receipt/import/continue" && request.method === "POST") {
        const job_id = String(url.searchParams.get("job_id")||"").trim();
        const batch = clamp(url.searchParams.get("batch"),1,200,50);
        const job = await getJob(env, job_id);
        if (!job) return json({ ok:false, error:"job_not_found" }, 404);

        const csvObj = await env.RECEIPTS_BUCKET.get(`uploads/${job_id}.csv`);
        if (!csvObj) return json({ ok:false, error:"uploaded_csv_not_found" }, 404);
        const rows = parseCsv(await csvObj.text());

        const start = Number(job.next_index||0);
        const end = Math.min(rows.length, start + batch);
        let ok=0, ng=0;

        for (let i=start; i<end; i++) {
          const r = rows[i];
          const year = normYear(r.year) ?? job.year;
          const cents = toCents(r.amount);

          if (!r.member_id || !r.branch || !Number.isFinite(cents) || !year) {
            ng++;
            await upsertAnnual(env,{year,member_id:r.member_id||"(missing)",branch:r.branch||"(missing)",name:"(invalid)",amount_cents:0,issue_date:today(),pdf_key:"",status:"ERROR",error:"invalid_row"});
            continue;
          }

          const hs = await hubspotGetContactByIdProperty(env, r.member_id, "member_id", ["firstname","lastname","email","receipt_years_available"]);
          if (!hs.ok) {
            ng++;
            await upsertAnnual(env,{year,member_id:r.member_id,branch:r.branch,name:"(HubSpot not found)",amount_cents:cents,issue_date:today(),pdf_key:"",status:"ERROR",error:"hubspot_not_found"});
            continue;
          }
          const p = (await hs.json()).properties || {};
          const email = normEmail(p.email);
          if (!email) {
            ng++;
            await upsertAnnual(env,{year,member_id:r.member_id,branch:r.branch,name:fmtName(p.firstname,p.lastname,p.email),amount_cents:cents,issue_date:today(),pdf_key:"",status:"ERROR",error:"missing_email"});
            continue;
          }

          const name = fmtName(p.firstname,p.lastname,p.email);

          // HubSpot update (eligible + years)
          const years = parseYears(p.receipt_years_available);
          if (!years.includes(String(year))) years.push(String(year));
          await hubspotPatchContactByIdProperty(env, r.member_id, "member_id", {
            receipt_portal_eligible: true,
            receipt_years_available: years.join(";")
          });

          // PDF
          const pdf_key = `receipts/${r.member_id}/${year}.pdf`;
          const pdf = await generateReceiptPdf(env,{name,year:String(year),amount:(cents/100).toFixed(2),date:today()});
          await env.RECEIPTS_BUCKET.put(pdf_key, pdf, { httpMetadata:{ contentType:"application/pdf" }});

          await upsertAnnual(env,{year,member_id:r.member_id,branch:r.branch,name,amount_cents:cents,issue_date:today(),pdf_key,status:"DONE",error:null});
          ok++;
        }

        await env.RECEIPTS_DB.prepare(`
          UPDATE receipt_import_job
          SET processed_rows=?, ok_rows=ok_rows+?, ng_rows=ng_rows+?, next_index=?,
              status=CASE WHEN ?>=total_rows THEN 'DONE' ELSE 'RUNNING' END,
              updated_at=datetime('now')
          WHERE job_id=?
        `).bind(end, ok, ng, end, end, job_id).run();

        const updated = await getJob(env, job_id);
        return json({ ok:true, job: updated, done: updated.status==="DONE", batch:{start,end,ok,ng} });
      }

      // -------- REBUILD --------
      if (path === "/api/admin/receipt/rebuild" && request.method === "POST") {
        const year = normYear(url.searchParams.get("year"));
        const member_id = String(url.searchParams.get("member_id")||"").trim();
        const branch = String(url.searchParams.get("branch")||"ALL").trim();
        if (!year) return json({ ok:false, error:"year_required" }, 400);

        let q = `SELECT year, member_id, branch, name, amount_cents, issue_date, pdf_key FROM receipt_annual WHERE year=?`;
        const args = [year];
        if (member_id) { q += ` AND member_id=?`; args.push(member_id); }
        else if (branch && branch !== "ALL") { q += ` AND branch=?`; args.push(branch); }

        const rows = await env.RECEIPTS_DB.prepare(q).bind(...args).all();
        let rebuilt=0;
        for (const r of rows.results||[]) {
          const pdf = await generateReceiptPdf(env,{name:r.name,year:String(r.year),amount:(Number(r.amount_cents)/100).toFixed(2),date:r.issue_date});
          await env.RECEIPTS_BUCKET.put(r.pdf_key,pdf,{httpMetadata:{contentType:"application/pdf"}});
          rebuilt++;
        }
        return json({ ok:true, rebuilt });
      }

      // -------- MAIL APIs --------
      if (path === "/api/admin/receipt/email/send-one" && request.method === "POST") {
        const body = await request.json().catch(() => null);
        const member_id = String(body?.member_id||"").trim();
        const year = normYear(body?.year);
        if (!member_id || !year) return json({ ok:false, error:"member_id_and_year_required" }, 400);

        const row = await env.RECEIPTS_DB.prepare(`SELECT * FROM receipt_annual WHERE year=? AND member_id=?`).bind(year,member_id).first();
        if (!row) return json({ ok:false, error:"row_not_found" }, 404);
        if (String(row.status).toUpperCase() !== "DONE") return json({ ok:false, error:"not_done" }, 409);

        const email = await getEmailByMemberId(env, member_id);
        if (!email) return json({ ok:false, error:"email_not_found_in_hubspot" }, 404);

        await sendReceiptNoticeEmail(env,{to:email,name:row.name,year:row.year,amount_cents:row.amount_cents});
        return json({ ok:true, sent:1, to:email, member_id, year });
      }

      if (path === "/api/admin/receipt/email/send-bulk" && request.method === "POST") {
        const body = await request.json().catch(() => null);
        const year = normYear(body?.year);
        const branch = String(body?.branch || "ALL").trim();
        if (!year) return json({ ok:false, error:"year_required" }, 400);

        let q = `SELECT year, member_id, branch, name, amount_cents, status FROM receipt_annual WHERE year=? AND status='DONE'`;
        const args = [year];
        if (branch !== "ALL") { q += ` AND branch=?`; args.push(branch); }
        q += ` ORDER BY branch, name`;

        const rows = await env.RECEIPTS_DB.prepare(q).bind(...args).all();
        const list = rows.results || [];

        let sent_ok=0, sent_ng=0;
        const results = [];

        for (const r of list) {
          const email = await getEmailByMemberId(env, r.member_id);
          if (!email) { sent_ng++; results.push({member_id:r.member_id, ok:false, error:"email_not_found"}); continue; }
          try{
            await sendReceiptNoticeEmail(env,{to:email,name:r.name,year:r.year,amount_cents:r.amount_cents});
            sent_ok++; results.push({member_id:r.member_id, ok:true});
          }catch(e){
            sent_ng++; results.push({member_id:r.member_id, ok:false, error:String(e?.message||e)});
          }
        }

        return json({ ok:true, year, branch, targets:list.length, sent_ok, sent_ng, results });
      }

      return json({ ok:false, error:"not_found" }, 404);

    } catch (e) {
      return json({ ok:false, error:"server_error", detail:String(e?.stack||e) }, 500);
    }
  }
};

/* ---------------- EMAIL (Resend) ---------------- */
async function sendReceiptNoticeEmail(env, { to, name, year, amount_cents }) {
  const portal = (env.PORTAL_ORIGIN || "https://kamikumite.worlddivinelight.org") + "/receipt/";
  const subject = `Annual Donation Receipt (${year}) – World Divine Light`;
  const amount = (Number(amount_cents) / 100).toFixed(2);

  const html = `
  <div style="font-family:Arial,sans-serif;line-height:1.6">
    <p>Dear ${escapeHtml(name || "Member")},</p>
    <p>This email is from World Divine Light regarding your Annual Donation Receipt.</p>
    <p>Unless otherwise requested, the receipt reflects the total amount of donations you made during the calendar year <b>${escapeHtml(String(year))}</b>.</p>
    <p><b>Total amount:</b> $${escapeHtml(amount)}</p>
    <p>Please download your receipt from the link below and use it as needed for your tax filing for this year.</p>
    <p>You will be asked to sign in with your email address and a one-time verification code.</p>
    <p><a href="${portal}">${portal}</a></p>
    <p>— World Divine Light</p>
  </div>`;
  await sendResend(env,{to,subject,html});
}

async function sendResend(env, { to, subject, html }) {
  const apiKey = must(env.RESEND_API_KEY, "Missing RESEND_API_KEY");
  const from = must(env.MAIL_FROM, "Missing MAIL_FROM");
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ from, to: [to], subject, html })
  });
  if (!res.ok) throw new Error(`resend_failed: ${res.status} ${await res.text()}`);
}

function otpHtml(code){
  return `<div style="font-family:Arial,sans-serif;line-height:1.6">
    <p>Your verification code is:</p>
    <p style="font-size:24px;font-weight:800;letter-spacing:2px">${escapeHtml(code)}</p>
    <p>This code will expire in 10 minutes.</p>
    <p>— World Divine Light</p>
  </div>`;
}

/* ---------------- HUBSPOT ---------------- */
function hsHeaders(env){
  const token = must(env.HUBSPOT_ACCESS_TOKEN, "Missing HUBSPOT_ACCESS_TOKEN");
  return { authorization:`Bearer ${token}`, "content-type":"application/json" };
}
async function hubspotGetContactByEmail(env,email,properties=[]){
  const u=new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(email)}`);
  u.searchParams.set("idProperty","email");
  if (properties.length) u.searchParams.set("properties",properties.join(","));
  return fetch(u.toString(),{headers:hsHeaders(env)});
}
async function hubspotGetContactByIdProperty(env,idValue,idProperty,properties=[]){
  const u=new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(idValue)}`);
  u.searchParams.set("idProperty",idProperty);
  if (properties.length) u.searchParams.set("properties",properties.join(","));
  return fetch(u.toString(),{headers:hsHeaders(env)});
}
async function hubspotPatchContactByIdProperty(env,idValue,idProperty,properties){
  const u=new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(idValue)}`);
  u.searchParams.set("idProperty",idProperty);
  return fetch(u.toString(),{method:"PATCH",headers:hsHeaders(env),body:JSON.stringify({properties})});
}
async function getEmailByMemberId(env, member_id){
  const hs = await hubspotGetContactByIdProperty(env, member_id, "member_id", ["email"]);
  if (!hs.ok) return null;
  const p = (await hs.json()).properties || {};
  return normEmail(p.email) || null;
}

/* ---------------- PDF ---------------- */
async function generateReceiptPdf(env,{name,year,amount,date}){
  const obj = await env.RECEIPTS_BUCKET.get("templates/receipt_template_v1.pdf");
  if (!obj) throw new Error("template_not_found");

  const cfg = await getTemplateConfig(env);
  const pdf = await PDFDocument.load(await obj.arrayBuffer());
  const page = pdf.getPages()[Math.max(0, Math.min(cfg.page||0, pdf.getPages().length-1))];

  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const size = Number(cfg.font_size||12);

  page.drawText(String(name),   { x:Number(cfg.name_x),   y:Number(cfg.name_y),   size, font, color:rgb(0,0,0) });
  page.drawText(String(year),   { x:Number(cfg.year_x),   y:Number(cfg.year_y),   size, font, color:rgb(0,0,0) });
  page.drawText(String(amount), { x:Number(cfg.amount_x), y:Number(cfg.amount_y), size, font, color:rgb(0,0,0) });
  page.drawText(String(date),   { x:Number(cfg.date_x),   y:Number(cfg.date_y),   size, font, color:rgb(0,0,0) });

  return await pdf.save();
}

/* ---------------- D1 ---------------- */
async function getTemplateConfig(env){
  const row = await env.RECEIPTS_DB.prepare(
    "SELECT page,name_x,name_y,year_x,year_y,amount_x,amount_y,date_x,date_y,font_size FROM receipt_template_config WHERE id=1"
  ).first();
  return row || {page:0,name_x:152,name_y:650,year_x:450,year_y:650,amount_x:410,amount_y:548,date_x:450,date_y:520,font_size:12};
}
async function getJob(env,job_id){
  return await env.RECEIPTS_DB.prepare("SELECT * FROM receipt_import_job WHERE job_id=?").bind(job_id).first();
}
async function upsertAnnual(env,row){
  await env.RECEIPTS_DB.prepare(`
    INSERT INTO receipt_annual(year,member_id,branch,name,amount_cents,issue_date,pdf_key,status,error,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))
    ON CONFLICT(year,member_id) DO UPDATE SET
      branch=excluded.branch,
      name=excluded.name,
      amount_cents=excluded.amount_cents,
      issue_date=excluded.issue_date,
      pdf_key=excluded.pdf_key,
      status=excluded.status,
      error=excluded.error,
      updated_at=datetime('now')
  `).bind(
    row.year,row.member_id,row.branch,row.name,row.amount_cents,row.issue_date,row.pdf_key,row.status,row.error
  ).run();
}

/* ---------------- CSV ---------------- */
function parseCsv(text){
  const lines = String(text||"").trim().split(/\r?\n/);
  if (lines.length < 2) return [];
  const header = lines[0].split(",").map(s=>s.trim());
  const idx = (k)=>header.indexOf(k);
  const iM=idx("member_id"), iB=idx("branch"), iA=idx("amount"), iY=idx("year");
  return lines.slice(1).filter(Boolean).map(line=>{
    const cols=line.split(",").map(s=>s.trim());
    return { member_id: cols[iM]||"", branch: cols[iB]||"", amount: cols[iA]||"", year: (iY>=0?cols[iY]:"") };
  });
}

/* ---------------- Common ---------------- */
function html(body,status=200){
  return new Response(body,{status,headers:{"content-type":"text/html; charset=utf-8","cache-control":"no-store"}});
}
function json(obj,status=200){
  return new Response(JSON.stringify(obj),{status,headers:{"content-type":"application/json; charset=utf-8","cache-control":"no-store"}});
}
function jsonC(obj,status,allow,origin){
  return new Response(JSON.stringify(obj),{status,headers:{...cors(allow,origin),"content-type":"application/json; charset=utf-8","cache-control":"no-store"}});
}
function cors(allow,origin){
  const o = allow;
  return {
    "access-control-allow-origin": o,
    "access-control-allow-credentials":"true",
    "access-control-allow-headers":"content-type",
    "access-control-allow-methods":"GET,POST,OPTIONS",
    "vary":"Origin"
  };
}
function must(v,msg){ if(!v) throw new Error(msg); return v; }
function normEmail(v){ const s=String(v||"").trim().toLowerCase(); return s.includes("@")?s:""; }
function toBool(v){ return v===true || String(v).toLowerCase()==="true"; }
function parseYears(v){ return String(v||"").split(";").map(s=>s.trim()).filter(Boolean); }
function today(){ return new Date().toISOString().slice(0,10); }
function normYear(v){ const n=parseInt(String(v||""),10); return (n>=2000 && n<=2100)?n:null; }
function clamp(v,min,max,def){ const n=parseInt(String(v||""),10); return isNaN(n)?def:Math.max(min,Math.min(max,n)); }
function toCents(v){ const n=Number(String(v||"").replace(/[$,]/g,"")); return Number.isFinite(n)?Math.round(n*100):NaN; }
async function sha256Hex(s){ const b=new TextEncoder().encode(s); const d=await crypto.subtle.digest("SHA-256",b); return [...new Uint8Array(d)].map(x=>x.toString(16).padStart(2,"0")).join(""); }

function makeSession(env,payload){
  const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
  const body = btoa(JSON.stringify({ ...payload, ts: Date.now() })).replaceAll("+","-").replaceAll("/","_").replaceAll("=","");
  const sig = btoa(`${secret}:${body}`).slice(0,32).replaceAll("+","-").replaceAll("/","_").replaceAll("=","");
  return `${body}.${sig}`;
}
function verifySession(env,token){
  const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
  if (!token) return { ok:false };
  const parts=String(token).split(".");
  if (parts.length!==2) return { ok:false };
  const body=parts[0], sig=parts[1];
  const expect=btoa(`${secret}:${body}`).slice(0,32).replaceAll("+","-").replaceAll("/","_").replaceAll("=","");
  if (sig!==expect) return { ok:false };
  try{
    const jsonStr = atob(body.replaceAll("-","+").replaceAll("_","/"));
    const obj = JSON.parse(jsonStr);
    return { ok:true, email: obj.email, member_id: obj.member_id };
  }catch{ return { ok:false }; }
}
function readCookie(cookieHeader,name){
  const m=String(cookieHeader||"").match(new RegExp("(^|;\\s*)"+name+"=([^;]+)"));
  return m?m[2]:"";
}
function escapeHtml(s){
  return String(s??"").replace(/[&<>"']/g,(c)=>({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c]));
}
function fmtName(fn,ln,email){
  const f=String(fn||"").trim(), l=String(ln||"").trim();
  return (f+" "+l).trim() || (normEmail(email).split("@")[0] || "Member");
}

/* ---------------- Member UI ---------------- */
function memberPortalHtml(){
  return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Receipt Portal</title></head><body style="font-family:system-ui;padding:24px">
  <h2>Receipt Portal</h2><p>OK</p></body></html>`;
}
