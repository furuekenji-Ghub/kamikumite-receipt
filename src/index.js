import { PDFDocument, StandardFonts, rgb } from "pdf-lib";

/**
 * RECEIPT SYSTEM — FULL BUILD
 * - CSV import -> D1 -> PDF generate -> R2 save
 * - Member portal (OTP) -> download PDF
 * - Admin mail send: single & bulk (Resend)
 *
 * Required bindings:
 *   D1: RECEIPTS_DB
 *   R2: RECEIPTS_BUCKET (wdl-receipts)
 *   KV: OTP_KV
 *
 * Required secrets/vars:
 *   HUBSPOT_ACCESS_TOKEN (secret)
 *   RESEND_API_KEY (secret)
 *   MAIL_FROM (plaintext) e.g. World Divine Light <no-reply@worlddivinelight.org>
 *   SESSION_SECRET (secret)  (or MAGICLINK_SECRET)
 *   PORTAL_ORIGIN (plaintext) e.g. https://kamikumite.worlddivinelight.org  (optional)
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const host = url.hostname;
    const path = url.pathname;

    try {
      // =========================================================
      // MEMBER UI (English only)
      // =========================================================
      if (host === "kamikumite.worlddivinelight.org" && path.startsWith("/receipt")) {
        return html(memberPortalHtml(), 200);
      }

      // =========================================================
      // MEMBER API (OTP + me + pdf)
      // =========================================================
      if (host === "api.kamikumite.worlddivinelight.org" && path.startsWith("/api/receipt/")) {
        const allow = env.PORTAL_ORIGIN || "https://kamikumite.worlddivinelight.org";
        const origin = request.headers.get("Origin") || "";
        if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors(allow, origin) });

        if (path === "/api/receipt/request-code" && request.method === "POST") {
          if (!env.OTP_KV) return jsonC({ ok: false, error: "kv_not_bound" }, 501, allow, origin);

          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allow, origin);

          // Rate limit 60s per email
          const rlKey = `rl:${email}`;
          const rl = await env.OTP_KV.get(rlKey, "json");
          const now = Date.now();
          const last = Number(rl?.lastSendTs || 0);
          if (now - last < 60_000) return jsonC({ ok: false, error: "rate_limited" }, 429, allow, origin);

          const hs = await hubspotGetContactByEmail(env, email, ["member_id", "receipt_portal_eligible", "receipt_years_available"]);
          if (hs.status === 404) return jsonC({ ok: false, error: "not_registered" }, 404, allow, origin);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow, origin);

          const p = (await hs.json()).properties || {};
          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allow, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(String(p.receipt_years_available || "").trim());
          if (!memberId) return jsonC({ ok: false, error: "member_id_missing" }, 403, allow, origin);
          if (!years.length) return jsonC({ ok: false, error: "no_years" }, 403, allow, origin);

          const code = String(Math.floor(100000 + Math.random() * 900000));
          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET (or MAGICLINK_SECRET)");
          const hash = await sha256Hex(`${code}:${secret}`);

          await env.OTP_KV.put(`otp:${email}`, JSON.stringify({
            hash,
            member_id: memberId,
            years,
            exp: now + 10 * 60_000,
            attempts: 0
          }), { expirationTtl: 10 * 60 });

          await env.OTP_KV.put(rlKey, JSON.stringify({ lastSendTs: now }), { expirationTtl: 120 });

          await sendResend(env, {
            to: email,
            subject: "Your verification code for the Receipt Portal",
            html: otpHtml(code)
          });

          return jsonC({ ok: true, sent: true }, 200, allow, origin);
        }

        if (path === "/api/receipt/verify-code" && request.method === "POST") {
          if (!env.OTP_KV) return jsonC({ ok: false, error: "kv_not_bound" }, 501, allow, origin);

          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          const code = String(body?.code || "").trim();
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allow, origin);
          if (!/^\d{6}$/.test(code)) return jsonC({ ok: false, error: "code_invalid" }, 400, allow, origin);

          const rec = await env.OTP_KV.get(`otp:${email}`, "json");
          if (!rec) return jsonC({ ok: false, error: "code_expired" }, 401, allow, origin);

          const now = Date.now();
          if (now > Number(rec.exp || 0)) return jsonC({ ok: false, error: "code_expired" }, 401, allow, origin);

          const attempts = Number(rec.attempts || 0);
          if (attempts >= 5) return jsonC({ ok: false, error: "too_many_attempts" }, 429, allow, origin);

          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET (or MAGICLINK_SECRET)");
          const hash = await sha256Hex(`${code}:${secret}`);
          if (hash !== rec.hash) {
            rec.attempts = attempts + 1;
            await env.OTP_KV.put(`otp:${email}`, JSON.stringify(rec), { expirationTtl: 10 * 60 });
            return jsonC({ ok: false, error: "code_wrong" }, 401, allow, origin);
          }

          await env.OTP_KV.delete(`otp:${email}`);

          const session = makeSession(env, { email, member_id: rec.member_id });
          const cookie = `receipt_session=${session}; Path=/; Secure; HttpOnly; SameSite=Lax; Domain=.worlddivinelight.org; Max-Age=${7 * 86400}`;

          return new Response(JSON.stringify({ ok: true, years: rec.years || [] }), {
            status: 200,
            headers: {
              ...cors(allow, origin),
              "content-type": "application/json; charset=utf-8",
              "cache-control": "no-store",
              "set-cookie": cookie
            }
          });
        }

        if (path === "/api/receipt/me" && request.method === "GET") {
          const s = verifySession(env, readCookie(request.headers.get("Cookie") || "", "receipt_session"));
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allow, origin);

          const hs = await hubspotGetContactByEmail(env, s.email, ["receipt_years_available"]);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow, origin);

          const years = parseYears((await hs.json()).properties?.receipt_years_available);
          return jsonC({ ok: true, years }, 200, allow, origin);
        }

        if (path === "/api/receipt/pdf" && request.method === "GET") {
          const s = verifySession(env, readCookie(request.headers.get("Cookie") || "", "receipt_session"));
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allow, origin);

          const year = String(url.searchParams.get("year") || "").trim();
          if (!/^\d{4}$/.test(year)) return jsonC({ ok: false, error: "invalid_year" }, 400, allow, origin);

          const hs = await hubspotGetContactByEmail(env, s.email, ["member_id", "receipt_years_available", "receipt_portal_eligible"]);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow, origin);

          const p = (await hs.json()).properties || {};
          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allow, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(p.receipt_years_available);
          if (!memberId || !years.includes(year)) return jsonC({ ok: false, error: "year_not_available" }, 403, allow, origin);

          const key = `receipts/${memberId}/${year}.pdf`;
          const obj = await env.RECEIPTS_BUCKET.get(key);
          if (!obj) return jsonC({ ok: false, error: "pdf_not_found" }, 404, allow, origin);

          return new Response(obj.body, {
            status: 200,
            headers: {
              ...cors(allow, origin),
              "content-type": "application/pdf",
              "cache-control": "no-store",
              "content-disposition": `attachment; filename="receipt_${memberId}_${year}.pdf"`
            }
          });
        }

        return jsonC({ ok: false, error: "not_found" }, 404, allow, origin);
      }

      // =========================================================
      // ADMIN API (Access-protected)
      // =========================================================
      if (!path.startsWith("/api/admin/receipt/")) return json({ ok: false, error: "Not found" }, 404);

      if (path === "/api/admin/receipt/_version") {
        return json({ ok: true, worker: "kamikumite-receipt", build: "CSV_IMPORT_v1+PDFGEN_v1+MAIL_v1+REBUILD_v1" });
      }

      // -------- Dashboard (D1) ----------
      if (path === "/api/admin/receipt/dashboard" && request.method === "GET") {
        const year = normYear(url.searchParams.get("year")) ?? (new Date().getFullYear() - 1);
        const rows = await env.RECEIPTS_DB.prepare(`
          SELECT year, member_id, branch, name, amount_cents, issue_date, pdf_key, status, error
          FROM receipt_annual
          WHERE year = ?
          ORDER BY branch ASC, name ASC
        `).bind(year).all();

        return json({ ok: true, year, rows: rows.results || [] });
      }

      // -------- Admin PDF view ----------
      if (path === "/api/admin/receipt/pdf" && request.method === "GET") {
        const member_id = String(url.searchParams.get("member_id") || "").trim();
        const year = String(url.searchParams.get("year") || "").trim();
        if (!member_id || !/^\d{4}$/.test(year)) return json({ ok: false, error: "member_id_and_year_required" }, 400);

        const key = `receipts/${member_id}/${year}.pdf`;
        const obj = await env.RECEIPTS_BUCKET.get(key);
        if (!obj) return json({ ok: false, error: "pdf_not_found", key }, 404);

        return new Response(obj.body, {
          status: 200,
          headers: { "content-type": "application/pdf", "cache-control": "no-store", "content-disposition": "inline" }
        });
      }

      // -------- Import (CSV) ----------
      if (path === "/api/admin/receipt/import" && request.method === "POST") {
        const csvText = await request.text();
        const rows = parseCsv(csvText);
        if (!rows.length) return json({ ok: false, error: "no_rows" }, 400);

        const job_id = crypto.randomUUID();
        const year = normYear(rows[0].year) ?? (new Date().getFullYear() - 1);

        await env.RECEIPTS_DB.prepare(`
          INSERT INTO receipt_import_job(job_id,year,total_rows,processed_rows,ok_rows,ng_rows,next_index,status)
          VALUES(?,?,?,0,0,0,0,'READY')
        `).bind(job_id, year, rows.length).run();

        await env.RECEIPTS_BUCKET.put(`uploads/${job_id}.csv`, csvText, { httpMetadata: { contentType: "text/csv" } });

        return json({ ok: true, job_id, year, total_rows: rows.length });
      }

      if (path === "/api/admin/receipt/import/continue" && request.method === "POST") {
        const job_id = String(url.searchParams.get("job_id") || "").trim();
        const batch = clamp(url.searchParams.get("batch"), 1, 100, 50);
        if (!job_id) return json({ ok: false, error: "job_id_required" }, 400);

        const job = await getJob(env, job_id);
        if (!job) return json({ ok: false, error: "job_not_found" }, 404);

        const csvObj = await env.RECEIPTS_BUCKET.get(`uploads/${job_id}.csv`);
        if (!csvObj) return json({ ok: false, error: "uploaded_csv_not_found" }, 404);
        const rows = parseCsv(await csvObj.text());

        const start = Number(job.next_index || 0);
        const end = Math.min(rows.length, start + batch);

        let ok = 0, ng = 0;

        for (let i = start; i < end; i++) {
          const r = rows[i];
          const year = normYear(r.year) ?? job.year;
          const cents = toCents(r.amount);

          if (!r.member_id || !r.branch || !Number.isFinite(cents)) {
            ng++;
            await upsertAnnual(env, {
              year,
              member_id: r.member_id || "(missing)",
              branch: r.branch || "(missing)",
              name: "(invalid)",
              amount_cents: 0,
              issue_date: today(),
              pdf_key: "",
              status: "ERROR",
              error: "invalid_row"
            });
            continue;
          }

          // HubSpot: lookup by member_id
          const hs = await hubspotGetContactByIdProperty(env, r.member_id, "member_id", [
            "firstname","lastname","email","receipt_years_available","receipt_portal_eligible"
          ]);
          if (!hs.ok) {
            ng++;
            await upsertAnnual(env, {
              year,
              member_id: r.member_id,
              branch: r.branch,
              name: "(HubSpot not found)",
              amount_cents: cents,
              issue_date: today(),
              pdf_key: "",
              status: "ERROR",
              error: "hubspot_not_found"
            });
            continue;
          }

          const p = (await hs.json()).properties || {};
          const name = fmtName(p.firstname, p.lastname, p.email);

          // Update HubSpot eligibility + years
          const years = parseYears(p.receipt_years_available);
          if (!years.includes(String(year))) years.push(String(year));

          await hubspotPatchContactByIdProperty(env, r.member_id, "member_id", {
            receipt_portal_eligible: true,
            receipt_years_available: years.join(";")
          });

          // Generate PDF -> R2
          const pdf_key = `receipts/${r.member_id}/${year}.pdf`;
          const pdf = await generateReceiptPdf(env, {
            name,
            year: String(year),
            amount: (cents / 100).toFixed(2),
            date: today()
          });

          await env.RECEIPTS_BUCKET.put(pdf_key, pdf, { httpMetadata: { contentType: "application/pdf" } });

          // Upsert D1
          await upsertAnnual(env, {
            year,
            member_id: r.member_id,
            branch: r.branch,
            name,
            amount_cents: cents,
            issue_date: today(),
            pdf_key,
            status: "DONE",
            error: null
          });

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
        return json({ ok: true, job: updated, done: updated.status === "DONE", batch: { start, end, ok, ng } });
      }

      // -------- Rebuild (CSVなし復旧) ----------
      if (path === "/api/admin/receipt/rebuild" && request.method === "POST") {
        const year = normYear(url.searchParams.get("year"));
        const member_id = String(url.searchParams.get("member_id") || "").trim();
        const branch = String(url.searchParams.get("branch") || "").trim();

        if (!year) return json({ ok: false, error: "year_required" }, 400);

        let q = `SELECT year, member_id, branch, name, amount_cents, issue_date, pdf_key FROM receipt_annual WHERE year=?`;
        const args = [year];

        if (member_id) { q += ` AND member_id=?`; args.push(member_id); }
        else if (branch && branch !== "ALL") { q += ` AND branch=?`; args.push(branch); }

        const rows = await env.RECEIPTS_DB.prepare(q).bind(...args).all();

        let rebuilt = 0;
        for (const r of rows.results || []) {
          const pdf = await generateReceiptPdf(env, {
            name: r.name,
            year: String(r.year),
            amount: (Number(r.amount_cents) / 100).toFixed(2),
            date: r.issue_date
          });
          await env.RECEIPTS_BUCKET.put(r.pdf_key, pdf, { httpMetadata: { contentType: "application/pdf" } });
          rebuilt++;
        }

        return json({ ok: true, rebuilt });
      }

      // =========================================================
      // ✅ MAIL API (NEW) — Individual & Bulk
      // =========================================================
      // POST /api/admin/receipt/email/send-one  { member_id, year }
      if (path === "/api/admin/receipt/email/send-one" && request.method === "POST") {
        const body = await request.json().catch(() => null);
        const member_id = String(body?.member_id || "").trim();
        const year = normYear(body?.year);
        if (!member_id || !year) return json({ ok: false, error: "member_id_and_year_required" }, 400);

        const row = await env.RECEIPTS_DB.prepare(`
          SELECT year, member_id, branch, name, amount_cents, issue_date, pdf_key, status, error
          FROM receipt_annual
          WHERE year=? AND member_id=?
        `).bind(year, member_id).first();

        if (!row) return json({ ok: false, error: "row_not_found" }, 404);
        if (String(row.status).toUpperCase() !== "DONE") return json({ ok: false, error: "not_done" }, 409);

        const email = await getEmailByMemberId(env, member_id);
        if (!email) return json({ ok: false, error: "email_not_found_in_hubspot" }, 404);

        await sendReceiptNoticeEmail(env, {
          to: email,
          name: row.name,
          year: row.year,
          amount_cents: row.amount_cents
        });

        return json({ ok: true, sent: 1, to: email, member_id, year });
      }

      // POST /api/admin/receipt/email/send-bulk { year, branch: "ALL"|"LA"|... }
      if (path === "/api/admin/receipt/email/send-bulk" && request.method === "POST") {
        const body = await request.json().catch(() => null);
        const year = normYear(body?.year);
        const branch = String(body?.branch || "ALL").trim();
        if (!year) return json({ ok: false, error: "year_required" }, 400);

        let q = `
          SELECT year, member_id, branch, name, amount_cents, status
          FROM receipt_annual
          WHERE year=? AND status='DONE'
        `;
        const args = [year];

        if (branch && branch !== "ALL") { q += ` AND branch=?`; args.push(branch); }

        q += ` ORDER BY branch ASC, name ASC`;

        const rows = await env.RECEIPTS_DB.prepare(q).bind(...args).all();
        const list = rows.results || [];

        let sent_ok = 0, sent_ng = 0;
        const results = [];

        for (const r of list) {
          const email = await getEmailByMemberId(env, r.member_id);
          if (!email) {
            sent_ng++;
            results.push({ member_id: r.member_id, ok: false, error: "email_not_found" });
            continue;
          }
          try {
            await sendReceiptNoticeEmail(env, {
              to: email,
              name: r.name,
              year: r.year,
              amount_cents: r.amount_cents
            });
            sent_ok++;
            results.push({ member_id: r.member_id, ok: true });
          } catch (e) {
            sent_ng++;
            results.push({ member_id: r.member_id, ok: false, error: String(e?.message || e) });
          }
        }

        return json({ ok: true, year, branch, targets: list.length, sent_ok, sent_ng, results });
      }

      return json({ ok: false, error: "not_found" }, 404);

    } catch (e) {
      return json({ ok: false, error: "server_error", detail: String(e?.stack || e) }, 500);
    }
  }
};

/* ---------------------------- EMAIL (Resend) ---------------------------- */

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

  await sendResend(env, { to, subject, html });
}

async function sendResend(env, { to, subject, html }) {
  const apiKey = must(env.RESEND_API_KEY, "Missing RESEND_API_KEY");
  const from = must(env.MAIL_FROM, "Missing MAIL_FROM");

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ from, to: [to], subject, html })
  });

  if (!res.ok) {
    throw new Error(`resend_failed: ${res.status} ${await res.text()}`);
  }
}

function otpHtml(code) {
  return `
  <div style="font-family:Arial,sans-serif;line-height:1.6">
    <p>Your verification code is:</p>
    <p style="font-size:24px;font-weight:800;letter-spacing:2px">${escapeHtml(code)}</p>
    <p>This code will expire in 10 minutes.</p>
    <p>— World Divine Light</p>
  </div>`;
}

/* ---------------------------- HUBSPOT ---------------------------- */

function hsHeaders(env) {
  const token = must(env.HUBSPOT_ACCESS_TOKEN, "Missing HUBSPOT_ACCESS_TOKEN");
  return { authorization: `Bearer ${token}`, "content-type": "application/json" };
}

async function hubspotGetContactByEmail(env, email, properties = []) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(email)}`);
  u.searchParams.set("idProperty", "email");
  if (properties.length) u.searchParams.set("properties", properties.join(","));
  return fetch(u.toString(), { headers: hsHeaders(env) });
}

async function hubspotGetContactByIdProperty(env, idValue, idProperty, properties = []) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(idValue)}`);
  u.searchParams.set("idProperty", idProperty);
  if (properties.length) u.searchParams.set("properties", properties.join(","));
  return fetch(u.toString(), { headers: hsHeaders(env) });
}

async function hubspotPatchContactByIdProperty(env, idValue, idProperty, properties) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(idValue)}`);
  u.searchParams.set("idProperty", idProperty);
  return fetch(u.toString(), {
    method: "PATCH",
    headers: hsHeaders(env),
    body: JSON.stringify({ properties })
  });
}

async function getEmailByMemberId(env, member_id) {
  const hs = await hubspotGetContactByIdProperty(env, member_id, "member_id", ["email"]);
  if (!hs.ok) return null;
  const p = (await hs.json()).properties || {};
  const email = normEmail(p.email);
  return email || null;
}

/* ---------------------------- PDF GENERATION ---------------------------- */

async function generateReceiptPdf(env, { name, year, amount, date }) {
  const obj = await env.RECEIPTS_BUCKET.get("templates/receipt_template_v1.pdf");
  if (!obj) throw new Error("template_not_found");

  const cfg = await getTemplateConfig(env);
  const pdf = await PDFDocument.load(await obj.arrayBuffer());
  const page = pdf.getPages()[Math.max(0, Math.min(cfg.page || 0, pdf.getPages().length - 1))];

  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const size = Number(cfg.font_size || 12);

  page.drawText(String(name),   { x: Number(cfg.name_x),   y: Number(cfg.name_y),   size, font, color: rgb(0,0,0) });
  page.drawText(String(year),   { x: Number(cfg.year_x),   y: Number(cfg.year_y),   size, font, color: rgb(0,0,0) });
  page.drawText(String(amount), { x: Number(cfg.amount_x), y: Number(cfg.amount_y), size, font, color: rgb(0,0,0) });
  page.drawText(String(date),   { x: Number(cfg.date_x),   y: Number(cfg.date_y),   size, font, color: rgb(0,0,0) });

  return await pdf.save();
}

async function getTemplateConfig(env) {
  const row = await env.RECEIPTS_DB.prepare(
    "SELECT page,name_x,name_y,year_x,year_y,amount_x,amount_y,date_x,date_y,font_size FROM receipt_template_config WHERE id=1"
  ).first();

  // fallback (should not happen if config exists)
  return row || {
    page: 0,
    name_x: 152, name_y: 650,
    year_x: 450, year_y: 650,
    amount_x: 410, amount_y: 548,
    date_x: 450, date_y: 520,
    font_size: 12
  };
}

/* ---------------------------- D1 HELPERS ---------------------------- */

async function getJob(env, job_id) {
  return await env.RECEIPTS_DB.prepare("SELECT * FROM receipt_import_job WHERE job_id=?").bind(job_id).first();
}

async function upsertAnnual(env, row) {
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
    row.year, row.member_id, row.branch, row.name, row.amount_cents, row.issue_date,
    row.pdf_key, row.status, row.error
  ).run();
}

/* ---------------------------- CSV ---------------------------- */

function parseCsv(text) {
  const lines = String(text || "").trim().split(/\r?\n/);
  if (lines.length < 2) return [];
  const header = lines[0].split(",").map(s => s.trim());
  const idx = (k) => header.indexOf(k);

  const iMember = idx("member_id");
  const iBranch = idx("branch");
  const iAmount = idx("amount");
  const iYear = idx("year");

  return lines.slice(1).map(line => {
    const cols = line.split(",").map(s => s.trim());
    return {
      member_id: cols[iMember] || "",
      branch: cols[iBranch] || "",
      amount: cols[iAmount] || "",
      year: (iYear >= 0 ? (cols[iYear] || "") : "")
    };
  });
}

/* ---------------------------- COMMON ---------------------------- */

function html(body, status = 200) {
  return new Response(body, { status, headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" } });
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
}

function jsonC(obj, status, allow, origin) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...cors(allow, origin), "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}

function cors(allow, origin) {
  const o = origin === allow ? allow : allow;
  return {
    "access-control-allow-origin": o,
    "access-control-allow-credentials": "true",
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "vary": "Origin"
  };
}

function must(v, msg) { if (!v) throw new Error(msg); return v; }
function normEmail(v) { const s = String(v || "").trim().toLowerCase(); return s.includes("@") ? s : ""; }
function toBool(v) { return v === true || String(v).toLowerCase() === "true"; }
function parseYears(s) { return String(s || "").split(";").map(x => x.trim()).filter(Boolean); }
function today() { return new Date().toISOString().slice(0, 10); }
function normYear(v) { const n = parseInt(String(v || ""), 10); return (n >= 2000 && n <= 2100) ? n : null; }
function clamp(v, min, max, def) { const n = parseInt(String(v || ""), 10); return isNaN(n) ? def : Math.max(min, Math.min(max, n)); }
function toCents(v) { const n = Number(String(v || "").replace(/[$,]/g, "")); return Number.isFinite(n) ? Math.round(n * 100) : NaN; }

async function sha256Hex(s) {
  const bytes = new TextEncoder().encode(s);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, "0")).join("");
}

function makeSession(env, payload) {
  const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET (or MAGICLINK_SECRET)");
  const body = btoa(JSON.stringify({ ...payload, ts: Date.now() })).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  const sig = btoa(`${secret}:${body}`).slice(0, 32).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  return `${body}.${sig}`;
}

function verifySession(env, token) {
  const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET (or MAGICLINK_SECRET)");
  if (!token) return { ok: false };
  const parts = String(token).split(".");
  if (parts.length !== 2) return { ok: false };
  const body = parts[0];
  const sig = parts[1];
  const expect = btoa(`${secret}:${body}`).slice(0, 32).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  if (sig !== expect) return { ok: false };

  try {
    const jsonStr = atob(body.replaceAll("-", "+").replaceAll("_", "/"));
    const obj = JSON.parse(jsonStr);
    return { ok: true, email: obj.email, member_id: obj.member_id };
  } catch {
    return { ok: false };
  }
}

function readCookie(cookieHeader, name) {
  const m = String(cookieHeader || "").match(new RegExp("(^|;\\s*)" + name + "=([^;]+)"));
  return m ? m[2] : "";
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => (
    { "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c]
  ));
}

/* ---------------------------- MEMBER UI (English) ---------------------------- */

function memberPortalHtml() {
  // Keep it simple; your current member UI can be swapped in later if desired.
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Receipt Portal</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:0;background:#f6f7f9;color:#111}
  header{background:#0b3c5d;color:#fff;padding:18px 20px}
  main{max-width:860px;margin:0 auto;padding:24px}
  .card{background:#fff;border:1px solid #e6e8ee;border-radius:12px;padding:18px;margin-bottom:14px}
  label{display:block;font-weight:800;margin-top:10px}
  input,button{width:100%;box-sizing:border-box;border:1px solid #d1d5db;border-radius:10px;padding:10px;font-size:14px;margin-top:6px}
  button{background:#0b3c5d;color:#fff;border:0;font-weight:900;cursor:pointer}
  button.secondary{background:#334155}
  .muted{color:#64748b;font-size:13px;margin-top:10px}
  .ok{background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46;border-radius:10px;padding:10px;margin-top:10px;white-space:pre-wrap}
  .ng{background:#fef2f2;border:1px solid #fecaca;color:#991b1b;border-radius:10px;padding:10px;margin-top:10px;white-space:pre-wrap}
  .years{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}
  a.btn{display:inline-block;background:#0b3c5d;color:#fff;text-decoration:none;border-radius:10px;padding:10px 14px;font-weight:900}
</style>
</head>
<body>
<header><h2 style="margin:0">Receipt Portal</h2></header>
<main>
  <div class="card">
    <div style="font-weight:900;font-size:16px">Login</div>
    <div class="muted">Enter your email to receive a verification code.</div>

    <label>Email address</label>
    <input id="email" type="email" placeholder="you@example.com"/>

    <button id="send">Send verification code</button>

    <label>Verification code</label>
    <input id="code" type="text" inputmode="numeric" placeholder="123456"/>

    <button id="verify" class="secondary">Verify & sign in</button>

    <div id="box" class="muted"></div>
  </div>

  <div class="card" id="cardYears" style="display:none">
    <div style="font-weight:900;font-size:16px">Available years</div>
    <div class="muted">Click a year to download your receipt (PDF).</div>
    <div class="years" id="years"></div>
  </div>
</main>

<script>
const API = "https://api.kamikumite.worlddivinelight.org";
const box = document.getElementById("box");
const cardYears = document.getElementById("cardYears");
const yearsEl = document.getElementById("years");

function ok(t){ box.innerHTML = '<div class="ok">'+t+'</div>'; }
function ng(t){ box.innerHTML = '<div class="ng">'+t+'</div>'; }

async function post(path, body){
  const r = await fetch(API+path, { method:"POST", headers:{ "Content-Type":"application/json" }, body: JSON.stringify(body), credentials:"include" });
  const t = await r.text();
  let j=null; try{ j=JSON.parse(t);}catch(e){}
  if(!r.ok) throw new Error((j && j.error ? j.error : "http_"+r.status));
  return j;
}

document.getElementById("send").onclick = async ()=>{
  const email = (document.getElementById("email").value||"").trim().toLowerCase();
  try{
    ok("Sending...");
    await post("/api/receipt/request-code", { email });
    ok("A 6-digit verification code has been sent to your email.");
  }catch(e){ ng(e.message); }
};

document.getElementById("verify").onclick = async ()=>{
  const email = (document.getElementById("email").value||"").trim().toLowerCase();
  const code = (document.getElementById("code").value||"").trim();
  try{
    ok("Verifying...");
    const r = await post("/api/receipt/verify-code", { email, code });
    ok("You're signed in.");
    yearsEl.innerHTML = "";
    (r.years||[]).forEach(y=>{
      const a=document.createElement("a");
      a.className="btn";
      a.href = API + "/api/receipt/pdf?year=" + encodeURIComponent(y);
      a.textContent = y;
      yearsEl.appendChild(a);
    });
    cardYears.style.display = "";
  }catch(e){ ng(e.message); }
};
</script>
</body>
</html>`;
}
