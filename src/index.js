import { PDFDocument, StandardFonts, rgb } from "pdf-lib";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const host = url.hostname;
    const pathname = url.pathname;

    try {
      // =========================================================
      // ① MEMBER UI (entrance page)  ※英語のみでOK
      // Route: kamikumite.worlddivinelight.org/receipt*
      // =========================================================
      if (host === "kamikumite.worlddivinelight.org" && pathname.startsWith("/receipt")) {
        return new Response(memberPortalHtml(env), {
          status: 200,
          headers: {
            "content-type": "text/html; charset=utf-8",
            "cache-control": "no-store",
          },
        });
      }

      // =========================================================
      // ② MEMBER API (OTP + me + pdf)
      // Route: api.kamikumite.worlddivinelight.org/api/receipt/*
      // =========================================================
      if (host === "api.kamikumite.worlddivinelight.org" && pathname.startsWith("/api/receipt/")) {
        const allowOrigin = env.PORTAL_ORIGIN || "https://kamikumite.worlddivinelight.org";
        const origin = request.headers.get("Origin") || "";

        if (request.method === "OPTIONS") {
          return new Response(null, { status: 204, headers: corsHeaders(allowOrigin, origin) });
        }

        // POST /api/receipt/request-code { email }
        if (pathname === "/api/receipt/request-code" && request.method === "POST") {
          if (!env.OTP_KV) return jsonC({ ok: false, error: "kv_not_bound" }, 501, allowOrigin, origin);

          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allowOrigin, origin);

          // rate limit per email (60 sec)
          const rlKey = `rl:${email}`;
          const rl = await env.OTP_KV.get(rlKey, "json");
          const now = Date.now();
          const last = Number(rl?.lastSendTs || 0);
          if (now - last < 60_000) {
            return jsonC({ ok: false, error: "rate_limited" }, 429, allowOrigin, origin);
          }

          // eligibility check
          const hs = await hubspotGetContactByEmail(env, email, [
            "email","member_id","receipt_portal_eligible","receipt_years_available"
          ]);
          if (hs.status === 404) return jsonC({ ok: false, error: "not_registered" }, 404, allowOrigin, origin);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed", detail: await safeText(hs) }, hs.status, allowOrigin, origin);

          const contact = await hs.json();
          const p = contact.properties || {};

          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allowOrigin, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(String(p.receipt_years_available || "").trim());
          if (!memberId) return jsonC({ ok: false, error: "member_id_missing" }, 403, allowOrigin, origin);
          if (!years.length) return jsonC({ ok: false, error: "no_years" }, 403, allowOrigin, origin);

          // create OTP
          const code = String(Math.floor(100000 + Math.random() * 900000));
          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET (or MAGICLINK_SECRET)");
          const otpHash = await sha256Hex(`${code}:${secret}`);

          await env.OTP_KV.put(`otp:${email}`, JSON.stringify({
            otpHash, memberId, years, expiresAt: now + 10 * 60_000, attempts: 0
          }), { expirationTtl: 10 * 60 });

          await env.OTP_KV.put(rlKey, JSON.stringify({ lastSendTs: now }), { expirationTtl: 120 });

          await sendResend(env, {
            to: email,
            subject: "Your verification code for the Receipt Portal",
            html: otpEmailHtml({ email, code })
          });

          return jsonC({ ok: true, sent: true }, 200, allowOrigin, origin);
        }

        // POST /api/receipt/verify-code { email, code }
        if (pathname === "/api/receipt/verify-code" && request.method === "POST") {
          if (!env.OTP_KV) return jsonC({ ok: false, error: "kv_not_bound" }, 501, allowOrigin, origin);

          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          const code = String(body?.code || "").trim();

          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allowOrigin, origin);
          if (!/^\d{6}$/.test(code)) return jsonC({ ok: false, error: "code_invalid" }, 400, allowOrigin, origin);

          const record = await env.OTP_KV.get(`otp:${email}`, "json");
          if (!record) return jsonC({ ok: false, error: "code_expired" }, 401, allowOrigin, origin);

          const now = Date.now();
          if (now > Number(record.expiresAt || 0)) return jsonC({ ok: false, error: "code_expired" }, 401, allowOrigin, origin);

          const attempts = Number(record.attempts || 0);
          if (attempts >= 5) return jsonC({ ok: false, error: "too_many_attempts" }, 429, allowOrigin, origin);

          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET (or MAGICLINK_SECRET)");
          const otpHash = await sha256Hex(`${code}:${secret}`);

          if (otpHash !== record.otpHash) {
            record.attempts = attempts + 1;
            await env.OTP_KV.put(`otp:${email}`, JSON.stringify(record), { expirationTtl: 10 * 60 });
            return jsonC({ ok: false, error: "code_wrong" }, 401, allowOrigin, origin);
          }

          await env.OTP_KV.delete(`otp:${email}`);

          const session = makeSession(env, { email, memberId: record.memberId });
          const cookie = [
            `receipt_session=${session}`,
            "Path=/",
            "Secure",
            "HttpOnly",
            "SameSite=Lax",
            "Domain=.worlddivinelight.org",
            `Max-Age=${7 * 24 * 60 * 60}`,
          ].join("; ");

          return new Response(JSON.stringify({
            ok: true,
            email,
            member_id: record.memberId,
            years: record.years || []
          }), {
            status: 200,
            headers: {
              ...corsHeaders(allowOrigin, origin),
              "content-type": "application/json; charset=utf-8",
              "cache-control": "no-store",
              "set-cookie": cookie
            }
          });
        }

        // GET /api/receipt/me
        if (pathname === "/api/receipt/me" && request.method === "GET") {
          const sess = readCookie(request.headers.get("Cookie") || "", "receipt_session");
          const s = verifySession(env, sess);
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allowOrigin, origin);

          const hs = await hubspotGetContactByEmail(env, s.email, [
            "email","member_id","receipt_portal_eligible","receipt_years_available"
          ]);
          if (hs.status === 404) return jsonC({ ok: false, error: "contact_not_found" }, 404, allowOrigin, origin);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed", detail: await safeText(hs) }, hs.status, allowOrigin, origin);

          const contact = await hs.json();
          const p = contact.properties || {};

          return jsonC({
            ok: true,
            email: (p.email || "").toLowerCase(),
            member_id: String(p.member_id || "").trim(),
            eligible: toBool(p.receipt_portal_eligible),
            years: parseYears(String(p.receipt_years_available || "").trim()),
          }, 200, allowOrigin, origin);
        }

        // GET /api/receipt/pdf?year=2024  (R2 receipts/{member_id}/{year}.pdf)
        if (pathname === "/api/receipt/pdf" && request.method === "GET") {
          const sess = readCookie(request.headers.get("Cookie") || "", "receipt_session");
          const s = verifySession(env, sess);
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allowOrigin, origin);

          const year = String(url.searchParams.get("year") || "").trim();
          if (!/^\d{4}$/.test(year)) return jsonC({ ok: false, error: "invalid_year" }, 400, allowOrigin, origin);

          const hs = await hubspotGetContactByEmail(env, s.email, [
            "member_id","receipt_portal_eligible","receipt_years_available"
          ]);
          if (hs.status === 404) return jsonC({ ok: false, error: "contact_not_found" }, 404, allowOrigin, origin);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed", detail: await safeText(hs) }, hs.status, allowOrigin, origin);

          const contact = await hs.json();
          const p = contact.properties || {};

          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allowOrigin, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(String(p.receipt_years_available || "").trim());
          if (!memberId) return jsonC({ ok: false, error: "member_id_missing" }, 403, allowOrigin, origin);
          if (!years.includes(year)) return jsonC({ ok: false, error: "year_not_available" }, 403, allowOrigin, origin);

          if (!env.RECEIPTS_BUCKET) return jsonC({ ok: false, error: "r2_not_bound" }, 501, allowOrigin, origin);

          const key = `receipts/${memberId}/${year}.pdf`;
          const obj = await env.RECEIPTS_BUCKET.get(key);
          if (!obj) return jsonC({ ok: false, error: "pdf_not_found", key }, 404, allowOrigin, origin);

          return new Response(obj.body, {
            status: 200,
            headers: {
              ...corsHeaders(allowOrigin, origin),
              "content-type": "application/pdf",
              "cache-control": "no-store",
              "content-disposition": `attachment; filename="receipt_${memberId}_${year}.pdf"`,
            },
          });
        }

        return jsonC({ ok: false, error: "not_found" }, 404, allowOrigin, origin);
      }

      // =========================================================
      // ③ ADMIN API + TEMPLATE TOOLING
      // =========================================================
      if (!pathname.startsWith("/api/admin/receipt/")) {
        return jsonPlain({ ok: false, error: "Not found" }, 404);
      }

      // version
      if (pathname === "/api/admin/receipt/_version" && request.method === "GET") {
        return jsonPlain({ ok: true, worker: "kamikumite-receipt", build: "GIT_BUNDLE_v1+TEMPLATE_TOOLS_v1" });
      }

      // ---- TEMPLATE CONFIG (D1) ----
      if (pathname === "/api/admin/receipt/template/config" && request.method === "GET") {
        const cfg = await getTemplateConfig(env);
        return jsonPlain({ ok: true, config: cfg });
      }

      if (pathname === "/api/admin/receipt/template/config" && request.method === "POST") {
        const body = await request.json().catch(() => null);
        if (!body) return jsonPlain({ ok: false, error: "invalid_json" }, 400);

        const cur = await getTemplateConfig(env);
        const next = {
          page: intOr(cur.page, body.page),
          name_x: numOr(cur.name_x, body.name_x),
          name_y: numOr(cur.name_y, body.name_y),
          year_x: numOr(cur.year_x, body.year_x),
          year_y: numOr(cur.year_y, body.year_y),
          amount_x: numOr(cur.amount_x, body.amount_x),
          amount_y: numOr(cur.amount_y, body.amount_y),
          date_x: numOr(cur.date_x, body.date_x),
          date_y: numOr(cur.date_y, body.date_y),
          font_size: numOr(cur.font_size, body.font_size),
        };

        await env.RECEIPTS_DB.prepare(`
          INSERT INTO receipt_template_config
            (id,page,name_x,name_y,year_x,year_y,amount_x,amount_y,date_x,date_y,font_size)
          VALUES
            (1,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(id) DO UPDATE SET
            page=excluded.page,
            name_x=excluded.name_x,
            name_y=excluded.name_y,
            year_x=excluded.year_x,
            year_y=excluded.year_y,
            amount_x=excluded.amount_x,
            amount_y=excluded.amount_y,
            date_x=excluded.date_x,
            date_y=excluded.date_y,
            font_size=excluded.font_size
        `).bind(
          next.page,
          next.name_x, next.name_y,
          next.year_x, next.year_y,
          next.amount_x, next.amount_y,
          next.date_x, next.date_y,
          next.font_size
        ).run();

        return jsonPlain({ ok: true, config: next });
      }

      // ---- TEMPLATE TEST PDF ----
      if (pathname === "/api/admin/receipt/template/test.pdf" && request.method === "GET") {
        if (!env.RECEIPTS_BUCKET) return jsonPlain({ ok: false, error: "R2_not_bound" }, 501);

        const obj = await env.RECEIPTS_BUCKET.get("templates/receipt_template_v1.pdf");
        if (!obj) return jsonPlain({ ok: false, error: "template_not_found", key: "templates/receipt_template_v1.pdf" }, 404);

        const name = (url.searchParams.get("name") || "John Doe").trim();
        const year = (url.searchParams.get("year") || String(new Date().getFullYear() - 1)).trim();
        const amount = (url.searchParams.get("amount") || "0").trim();
        const date = (url.searchParams.get("date") || new Date().toISOString().slice(0, 10)).trim();

        const cfg = await getTemplateConfig(env);
        const bytes = await obj.arrayBuffer();

        const pdf = await PDFDocument.load(bytes);
        const pages = pdf.getPages();
        const pageIndex = Math.max(0, Math.min(cfg.page, pages.length - 1));
        const page = pages[pageIndex];

        const font = await pdf.embedFont(StandardFonts.Helvetica);
        const size = cfg.font_size || 12;

        page.drawText(String(name),   { x: cfg.name_x,   y: cfg.name_y,   size, font, color: rgb(0,0,0) });
        page.drawText(String(year),   { x: cfg.year_x,   y: cfg.year_y,   size, font, color: rgb(0,0,0) });
        page.drawText(String(amount), { x: cfg.amount_x, y: cfg.amount_y, size, font, color: rgb(0,0,0) });
        page.drawText(String(date),   { x: cfg.date_x,   y: cfg.date_y,   size, font, color: rgb(0,0,0) });

        const out = await pdf.save();
        return new Response(out, {
          status: 200,
          headers: {
            "content-type": "application/pdf",
            "cache-control": "no-store",
            "content-disposition": `inline; filename="template_test.pdf"`,
          },
        });
      }

      // (ここに必要なら Adminの他エンドポイントを追加して拡張していきます)
      return jsonPlain({ ok: false, error: "Not found" }, 404);

    } catch (e) {
      return jsonPlain({ ok: false, error: "server_error", detail: String(e?.stack || e) }, 500);
    }
  },
};

/* =========================
   D1 helpers
========================= */
async function getTemplateConfig(env) {
  if (!env.RECEIPTS_DB) throw new Error("Missing D1 binding RECEIPTS_DB");
  const row = await env.RECEIPTS_DB.prepare(
    "SELECT page,name_x,name_y,year_x,year_y,amount_x,amount_y,date_x,date_y,font_size FROM receipt_template_config WHERE id=1"
  ).first();

  return row || {
    page: 0,
    name_x: 72, name_y: 650,
    year_x: 450, year_y: 650,
    amount_x: 450, amount_y: 560,
    date_x: 450, date_y: 520,
    font_size: 12
  };
}
function numOr(fallback, v) { const n = Number(v); return Number.isFinite(n) ? n : fallback; }
function intOr(fallback, v) { const n = Number.parseInt(String(v), 10); return Number.isFinite(n) ? n : fallback; }

/* =========================
   Common helpers
========================= */
function must(v, msg) { if (!v) throw new Error(msg); return v; }
function normEmail(v) { const s = String(v || "").trim().toLowerCase(); return s.includes("@") ? s : ""; }
async function safeText(res) { try { return await res.text(); } catch { return ""; } }
function normalizeYears(s) { return String(s).trim().replace(/\s+/g,"").replace(/,/g,";").replace(/;;+/g,";").replace(/;$/g,""); }
function parseYears(s) { const norm = normalizeYears(s); return norm ? norm.split(";").filter(Boolean) : []; }
function toBool(v) { if (typeof v === "boolean") return v; const s = String(v || "").toLowerCase().trim(); return s==="true"||s==="1"||s==="yes"; }
function readCookie(cookieHeader, name) {
  const parts = cookieHeader.split(";").map(s => s.trim());
  for (const p of parts) if (p.startsWith(name + "=")) return p.slice((name + "=").length);
  return "";
}
function corsHeaders(allowOrigin, origin) {
  const o = origin === allowOrigin ? allowOrigin : allowOrigin;
  return {
    "access-control-allow-origin": o,
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "Content-Type",
    "access-control-allow-credentials": "true",
    "vary": "Origin",
  };
}
function jsonPlain(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}
function jsonC(obj, status, allowOrigin, origin) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...corsHeaders(allowOrigin, origin), "content-type":"application/json; charset=utf-8", "cache-control":"no-store" },
  });
}
async function sha256Hex(input) {
  const enc = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", enc);
  return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, "0")).join("");
}
function makeSession(env, { email, memberId }) {
  const secret = env.SESSION_SECRET || env.MAGICLINK_SECRET;
  if (!secret) throw new Error("Missing SESSION_SECRET");
  const payloadB64 = btoa(JSON.stringify({ email, memberId, ts: Date.now() }))
    .replaceAll("+","-").replaceAll("/","_").replaceAll("=","");
  const sig = btoa(`${secret}:${payloadB64}`).slice(0, 32)
    .replaceAll("+","-").replaceAll("/","_").replaceAll("=","");
  return `${payloadB64}.${sig}`;
}
function verifySession(env, session) {
  const secret = env.SESSION_SECRET || env.MAGICLINK_SECRET;
  if (!secret || !session) return { ok:false };
  const parts = String(session).split(".");
  if (parts.length !== 2) return { ok:false };
  const payloadB64 = parts[0];
  const sig = parts[1];
  const expect = btoa(`${secret}:${payloadB64}`).slice(0, 32)
    .replaceAll("+","-").replaceAll("/","_").replaceAll("=","");
  if (sig !== expect) return { ok:false };
  let payload;
  try { payload = JSON.parse(atob(payloadB64.replaceAll("-","+").replaceAll("_","/"))); } catch { return { ok:false }; }
  const email = String(payload?.email||"").trim().toLowerCase();
  const memberId = String(payload?.memberId||"").trim();
  if (!email || !memberId) return { ok:false };
  return { ok:true, email, memberId };
}

/* =========================
   HubSpot + Resend
========================= */
function hsHeaders(env) {
  const token = must(env.HUBSPOT_ACCESS_TOKEN, "Missing HUBSPOT_ACCESS_TOKEN");
  return { authorization: `Bearer ${token}`, "content-type": "application/json" };
}
async function hubspotGetContactByEmail(env, email, properties) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(email)}`);
  u.searchParams.set("idProperty", "email");
  if (properties?.length) u.searchParams.set("properties", properties.join(","));
  return fetch(u.toString(), { headers: hsHeaders(env) });
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
function otpEmailHtml({ email, code }) {
  return `
  <div style="font-family:Arial,sans-serif;line-height:1.6">
    <p>Hello,</p>
    <p>Your verification code is:</p>
    <p style="font-size:24px;font-weight:800;letter-spacing:2px">${escapeHtml(code)}</p>
    <p>This code will expire in 10 minutes.</p>
    <p>If you didn’t request this code, you can safely ignore this email.</p>
    <p>— World Divine Light</p>
  </div>`;
}
function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c])
  );
}

/* =========================
   Member UI (English-only)
========================= */
function memberPortalHtml(env) {
  const api = "https://api.kamikumite.worlddivinelight.org";
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

    <button id="btnSend">Send verification code</button>

    <label>Verification code</label>
    <input id="code" type="text" inputmode="numeric" placeholder="123456"/>

    <button id="btnVerify" class="secondary">Verify & sign in</button>

    <div id="msg" class="muted"></div>
    <div id="box"></div>
  </div>

  <div class="card" id="cardYears" style="display:none">
    <div style="font-weight:900;font-size:16px">Available years</div>
    <div class="muted">Click a year to download your receipt (PDF).</div>
    <div class="years" id="years"></div>
  </div>
</main>

<script>
const API = ${JSON.stringify(api)};
const msg = document.getElementById("msg");
const box = document.getElementById("box");
const cardYears = document.getElementById("cardYears");
const yearsEl = document.getElementById("years");

function ok(t){ box.innerHTML = '<div class="ok">'+escapeHtml(t)+'</div>'; }
function ng(t){ box.innerHTML = '<div class="ng">'+escapeHtml(t)+'</div>'; }
function escapeHtml(s){ return String(s||"").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c])); }

async function post(path, body){
  const r = await fetch(API+path, {
    method:"POST",
    headers:{ "Content-Type":"application/json" },
    body: JSON.stringify(body),
    credentials: "include"
  });
  const t = await r.text();
  let j=null; try{ j=JSON.parse(t);}catch(e){}
  if(!r.ok) throw new Error((j && j.error ? j.error : "http_"+r.status)+"\\n"+t);
  return j;
}

document.getElementById("btnSend").onclick = async ()=>{
  const email = (document.getElementById("email").value||"").trim().toLowerCase();
  msg.textContent = "Sending...";
  box.innerHTML = "";
  try{
    await post("/api/receipt/request-code", { email });
    ok("A 6-digit verification code has been sent to your email.");
    msg.textContent = "Code sent";
  }catch(e){
    ng(String(e.message||e));
    msg.textContent = "Failed";
  }
};

document.getElementById("btnVerify").onclick = async ()=>{
  const email = (document.getElementById("email").value||"").trim().toLowerCase();
  const code = (document.getElementById("code").value||"").trim();
  msg.textContent = "Verifying...";
  box.innerHTML = "";
  try{
    const r = await post("/api/receipt/verify-code", { email, code });
    ok("You're signed in. Select a year below.");
    yearsEl.innerHTML = "";
    (r.years||[]).forEach(y=>{
      const a=document.createElement("a");
      a.className="btn";
      a.href = API + "/api/receipt/pdf?year=" + encodeURIComponent(y);
      a.textContent = y;
      yearsEl.appendChild(a);
    });
    cardYears.style.display = "";
    msg.textContent = "Signed in";
  }catch(e){
    ng(String(e.message||e));
    msg.textContent = "Failed";
  }
};
</script>
</body>
</html>`;
}
