import { PDFDocument, rgb } from "pdf-lib";
import fontkit from "@pdf-lib/fontkit";

/**
 * Receipt Worker — CSV_IMPORT + PDFGEN + MAIL + REBUILD + VALIDATE + QUEUES
 * - Admin UI/API: admin.mahikari.org (/api/admin/receipt/*)
 * - Member UI:    kamikumite.worlddivinelight.org/receipt
 * - Member API:   api.kamikumite.worlddivinelight.org/api/receipt/*
 * - Queue:        IMPORT_Q -> receipt-import-q
 */

// ===== PDF caches (global) =====
let _tmplCache = null; // ArrayBuffer
let _fontCache = null; // ArrayBuffer

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const host = url.hostname;
    const path = url.pathname;

    // --- _version (ALWAYS respond; deploy verification) ---
    if (path === "/api/admin/receipt/_version") {
      return json(
        {
          ok: true,
          worker: "kamikumite-receipt",
          build: "CSV_IMPORT+PDFGEN+MAIL+REBUILD+VALIDATE_v3+QUEUES_v1",
        },
        200
      );
    }

    try {
      /* =====================================================
       * MEMBER UI
       * ===================================================== */
      if (host === "kamikumite.worlddivinelight.org" && path.startsWith("/receipt")) {
        return html(memberPortalHtml(), 200);
      }

      /* =====================================================
       * MEMBER API
       * ===================================================== */
      if (host === "api.kamikumite.worlddivinelight.org" && path.startsWith("/api/receipt/")) {
        const allow = env.PORTAL_ORIGIN || "https://kamikumite.worlddivinelight.org";
        const origin = request.headers.get("Origin") || "";

        if (request.method === "OPTIONS") {
          return new Response(null, { status: 204, headers: cors(allow) });
        }

        // POST /api/receipt/request-code
        if (path === "/api/receipt/request-code" && request.method === "POST") {
          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allow);

          const hs = await hubspotGetContactByEmail(env, email, [
            "member_id",
            "receipt_portal_eligible",
            "receipt_years_available",
          ]);

          if (hs.status === 404) return jsonC({ ok: false, error: "not_registered" }, 404, allow);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow);

          const p = (await hs.json()).properties || {};
          if (!toBool(p.receipt_portal_eligible)) {
            return jsonC({ ok: false, error: "not_eligible" }, 403, allow);
          }

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(p.receipt_years_available);
          if (!memberId || !years.length) return jsonC({ ok: false, error: "not_ready" }, 403, allow);

          const code = String(Math.floor(100000 + Math.random() * 900000));
          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
          const hash = await sha256Hex(`${code}:${secret}`);

          await env.OTP_KV.put(
            `otp:${email}`,
            JSON.stringify({ hash, member_id: memberId, years, exp: Date.now() + 10 * 60_000, attempts: 0 }),
            { expirationTtl: 10 * 60 }
          );

          await sendResend(env, {
            to: email,
            subject: "Your verification code for the Receipt Portal",
            html: otpHtml(code),
          });

          return jsonC({ ok: true }, 200, allow);
        }

        // POST /api/receipt/verify-code
        if (path === "/api/receipt/verify-code" && request.method === "POST") {
          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          const code = String(body?.code || "").trim();
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allow);
          if (!/^\d{6}$/.test(code)) return jsonC({ ok: false, error: "code_invalid" }, 400, allow);

          const rec = await env.OTP_KV.get(`otp:${email}`, "json");
          if (!rec) return jsonC({ ok: false, error: "expired" }, 401, allow);
          if (Date.now() > Number(rec.exp || 0)) return jsonC({ ok: false, error: "expired" }, 401, allow);

          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
          const hash = await sha256Hex(`${code}:${secret}`);
          if (hash !== rec.hash) return jsonC({ ok: false, error: "wrong" }, 401, allow);

          await env.OTP_KV.delete(`otp:${email}`);

          const session = makeSession(env, { email, member_id: rec.member_id });
          const cookie = `receipt_session=${session}; Path=/; Secure; HttpOnly; SameSite=Lax; Domain=.worlddivinelight.org; Max-Age=${7 * 86400}`;

          return new Response(JSON.stringify({ ok: true, years: rec.years || [] }), {
            status: 200,
            headers: {
              ...cors(allow),
              "content-type": "application/json; charset=utf-8",
              "cache-control": "no-store",
              "set-cookie": cookie,
            },
          });
        }

        // GET /api/receipt/me
        if (path === "/api/receipt/me" && request.method === "GET") {
          const s = verifySession(env, readCookie(request.headers.get("Cookie") || "", "receipt_session"));
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allow);

          const hs = await hubspotGetContactByEmail(env, s.email, ["receipt_years_available"]);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow);

          const years = parseYears((await hs.json()).properties?.receipt_years_available);
          return jsonC({ ok: true, years }, 200, allow);
        }

        // GET /api/receipt/pdf?year=2025
        if (path === "/api/receipt/pdf" && request.method === "GET") {
          const s = verifySession(env, readCookie(request.headers.get("Cookie") || "", "receipt_session"));
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allow);

          const year = String(url.searchParams.get("year") || "").trim();
          if (!/^\d{4}$/.test(year)) return jsonC({ ok: false, error: "invalid_year" }, 400, allow);

          const hs = await hubspotGetContactByEmail(env, s.email, [
            "member_id",
            "receipt_years_available",
            "receipt_portal_eligible",
          ]);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow);

          const p = (await hs.json()).properties || {};
          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allow);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(p.receipt_years_available);
          if (!memberId || !years.includes(year)) return jsonC({ ok: false, error: "year_not_available" }, 403, allow);

          const key = `receipts/${memberId}/${year}.pdf`;
          const obj = await env.RECEIPTS_BUCKET.get(key);
          if (!obj) return jsonC({ ok: false, error: "pdf_not_found", key }, 404, allow);

          return new Response(obj.body, {
            status: 200,
            headers: {
              ...cors(allow),
              "content-type": "application/pdf",
              "cache-control": "no-store",
              "content-disposition": `attachment; filename="receipt_${memberId}_${year}.pdf"`,
            },
          });
        }

        return jsonC({ ok: false, error: "not_found" }, 404, allow);
      }

      /* =====================================================
       * ADMIN API
       * ===================================================== */
      if (path.startsWith("/api/admin/receipt/")) {
        // --- import/validate (ultra-light; never 500; no HubSpot) ---
        if (path === "/api/admin/receipt/import/validate" && request.method === "POST") {
          try {
            const ct = String(request.headers.get("content-type") || "").toLowerCase();

            let csvText = "";
            if (ct.includes("multipart/form-data")) {
              const fd = await request.formData();
              const f = fd.get("file") || fd.get("csv") || fd.get("upload");
              if (f && typeof f === "object" && "text" in f) csvText = await f.text();
              else return json({ ok: false, error: "csv_required" }, 200);
            } else {
              csvText = await request.text();
            }

            csvText = String(csvText || "");
            if (!csvText.trim()) return json({ ok: false, error: "no_rows" }, 200);

            // parse only head lines
            const headText = csvText.split(/\r\n|\n|\r/).slice(0, 1200).join("\n");
            const rows = parseCsv(headText);
            if (!rows.length) return json({ ok: false, error: "no_rows" }, 200);

            const errors = [];
            const maxCheck = 1000;
            const checkRows = rows.slice(0, maxCheck);

            for (let i = 0; i < checkRows.length; i++) {
              const r = checkRows[i];
              const rowNo = i + 2;
              const member_id = String(r.member_id || "").trim();
              const branch = String(r.branch || "").trim();
              const cents = parseMoneyToCents(r.amount);
              const year = normYear(r.year);

              if (!member_id) { errors.push({ type: "MISSING_MEMBER_ID", row: rowNo }); continue; }
              if (!branch) { errors.push({ type: "MISSING_BRANCH", row: rowNo, member_id }); continue; }
              if (cents === null) { errors.push({ type: "INVALID_AMOUNT", row: rowNo, member_id }); continue; }
              if (!year) { errors.push({ type: "INVALID_YEAR", row: rowNo, member_id }); continue; }
            }

            return json({
              ok: errors.length === 0,
              checked_rows: checkRows.length,
              total_rows: rows.length,
              hs_checked: 0,
              errors,
              warnings: [{ type: "HUBSPOT_CHECK_SKIPPED_IN_VALIDATE" }],
            }, 200);

          } catch (e) {
            return json({ ok: false, error: "validate_failed", warning: String(e?.message || e) }, 200);
          }
        }

        // --- import/start (Queues) ---
        if (path === "/api/admin/receipt/import/start" && request.method === "POST") {
          try {
            const ct = String(request.headers.get("content-type") || "").toLowerCase();

            let csvText = "";
            if (ct.includes("multipart/form-data")) {
              const fd = await request.formData();
              const f = fd.get("file") || fd.get("csv") || fd.get("upload");
              if (f && typeof f === "object" && "text" in f) csvText = await f.text();
              else return json({ ok: false, error: "csv_required" }, 400);
            } else {
              csvText = await request.text();
            }

            csvText = String(csvText || "");
            if (!csvText.trim()) return json({ ok: false, error: "empty_csv" }, 400);

            const job_id = crypto.randomUUID();

            // year: peek from head
            let year = (new Date()).getFullYear() - 1;
            try {
              const head = csvText.split(/\r\n|\n|\r/).slice(0, 10).join("\n");
              const headRows = parseCsv(head);
              const y = normYear(headRows?.[0]?.year);
              if (y) year = y;
            } catch {}

            const csv_key = `uploads/${job_id}.csv`;

            await env.RECEIPTS_BUCKET.put(csv_key, csvText, { httpMetadata: { contentType: "text/csv" } });

            // job row (phase/last_error columns may not exist; ignore if fails)
            await env.RECEIPTS_DB.prepare(`
              INSERT INTO receipt_import_job
                (job_id, year, total_rows, processed_rows, ok_rows, ng_rows, next_index, status, created_at, updated_at, csv_key)
              VALUES
                (?, ?, 0, 0, 0, 0, 0, 'RUNNING', datetime('now'), datetime('now'), ?)
            `).bind(job_id, year, csv_key).run();

            try {
              await env.RECEIPTS_DB.prepare(`UPDATE receipt_import_job SET phase='PARSING' WHERE job_id=?`).bind(job_id).run();
            } catch {}

            await env.IMPORT_Q.send({ type: "parse_csv", job_id });

            return json({ ok: true, job_id, year, status: "RUNNING" }, 200);
          } catch (e) {
            return json({ ok: false, error: "server_error", detail: String(e?.message || e) }, 500);
          }
        }

        // --- import/status ---
        if (path === "/api/admin/receipt/import/status" && request.method === "GET") {
          const job_id = String(url.searchParams.get("job_id") || "").trim();
          if (!job_id) return json({ ok: false, error: "job_id_required" }, 400);

          const job = await env.RECEIPTS_DB.prepare(`SELECT * FROM receipt_import_job WHERE job_id=?`).bind(job_id).first();
          if (!job) return json({ ok: false, error: "job_not_found" }, 404);

          return json({ ok: true, job }, 200);
        }

        // --- dashboard ---
        if (path === "/api/admin/receipt/dashboard" && request.method === "GET") {
          const year = normYear(url.searchParams.get("year")) ?? (new Date()).getFullYear() - 1;
          const rows = await env.RECEIPTS_DB.prepare(`
            SELECT year, member_id, branch, name, amount_cents, issue_date, pdf_key, status, error, email_status, email_sent_at
            FROM receipt_annual
            WHERE year=?
            ORDER BY branch, name
          `).bind(year).all();

          return json({ ok: true, year, rows: rows.results || [] }, 200);
        }

        // --- pdf view (admin) ---
        if (path === "/api/admin/receipt/pdf" && request.method === "GET") {
          const member_id = String(url.searchParams.get("member_id") || "").trim();
          const year = String(url.searchParams.get("year") || "").trim();
          if (!member_id || !/^\d{4}$/.test(year)) return json({ ok: false, error: "member_id_and_year_required" }, 400);

          const key = `receipts/${member_id}/${year}.pdf`;
          const obj = await env.RECEIPTS_BUCKET.get(key);
          if (!obj) return json({ ok: false, error: "pdf_not_found", key }, 404);

          return new Response(obj.body, {
            status: 200,
            headers: { "content-type": "application/pdf", "cache-control": "no-store", "content-disposition": "inline" },
          });
        }
        
        // --- bulk-delete-year ---
// body: { year: 2025, confirm: "DELETE 2025" }
if (path === "/api/admin/receipt/bulk-delete-year" && request.method === "POST") {
  const body = await request.json().catch(() => null);
  const year = normYear(body?.year);
  const confirm = String(body?.confirm || "").trim();

  const allowedYear = (new Date()).getFullYear() - 1;
  if (!year) return json({ ok:false, error:"year_required" }, 400);
  if (year !== allowedYear) return json({ ok:false, error:"year_not_allowed", allowedYear }, 403);

  const required = `DELETE ${year}`;
  if (confirm !== required) return json({ ok:false, error:"typed_confirmation_required", required }, 400);

  // PDF keys
  const rows = await env.RECEIPTS_DB.prepare(`SELECT pdf_key FROM receipt_annual WHERE year=?`).bind(year).all();
  const keys = (rows?.results || []).map(r => String(r.pdf_key || "").trim()).filter(Boolean);

  // DB delete
  await env.RECEIPTS_DB.prepare(`DELETE FROM receipt_annual WHERE year=?`).bind(year).run();

  // R2 delete
  let deleted_pdf = 0;
  for (const k of keys) {
    try { await env.RECEIPTS_BUCKET.delete(k); deleted_pdf++; } catch {}
  }

  return json({ ok:true, year, deleted_rows: keys.length, deleted_pdf }, 200);
}
        
        // --- delete-selected (single + multi) ---
        if (path === "/api/admin/receipt/delete-selected" && request.method === "POST") {
          const body = await request.json().catch(() => null);

          let selections =
            Array.isArray(body?.selections) ? body.selections :
            Array.isArray(body?.selected) ? body.selected :
            Array.isArray(body?.items) ? body.items :
            [];

          if (!selections.length) {
            const member_id = String(body?.member_id || body?.memberId || "").trim();
            const year = normYear(body?.year);
            if (member_id && year) selections = [{ member_id, year }];
          }

          if (!selections.length) return json({ ok: false, error: "selections_required" }, 400);

          let deleted_ok = 0, deleted_ng = 0;
          const results = [];

          for (const s of selections) {
            const member_id = String(s?.member_id || s?.memberId || "").trim();
            const year = normYear(s?.year);
            if (!member_id || !year) {
              deleted_ng++;
              results.push({ ok: false, member_id, year, error: "member_id_and_year_required" });
              continue;
            }

            const row = await env.RECEIPTS_DB
              .prepare(`SELECT pdf_key FROM receipt_annual WHERE year=? AND member_id=?`)
              .bind(year, member_id)
              .first();

            await env.RECEIPTS_DB
              .prepare(`DELETE FROM receipt_annual WHERE year=? AND member_id=?`)
              .bind(year, member_id)
              .run();

            const pdf_key = String(row?.pdf_key || "").trim();
            if (pdf_key) {
              try { await env.RECEIPTS_BUCKET.delete(pdf_key); } catch {}
            }

            deleted_ok++;
            results.push({ ok: true, member_id, year, deleted_pdf_key: pdf_key || null });
          }

          return json({ ok: true, deleted_ok, deleted_ng, results }, 200);
        }

        // --- admin fallback ---
        return json({ ok: false, error: "not_found" }, 404);
      }

      return json({ ok: false, error: "not_found" }, 404);

    } catch (e) {
      return json({ ok: false, error: "server_error", detail: String(e?.stack || e) }, 500);
    }
  },

  // =====================================================
  // Queue consumer
  // =====================================================
  async queue(batch, env, ctx) {
    for (const msg of batch.messages) {
      try {
        const body = msg.body || {};
        const type = String(body.type || "");
        const job_id = String(body.job_id || "").trim();
        if (!job_id) { msg.ack(); continue; }

        if (type === "parse_csv") {
          await handleParseCsv(env, job_id);
          msg.ack();
          continue;
        }
        if (type === "process_rows") {
          await handleProcessRows(env, job_id);
          msg.ack();
          continue;
        }

        msg.ack();
      } catch (e) {
        // retry (Queues)
        msg.retry();
      }
    }
  },
};

/* =========================================================
 * Helpers
 * ========================================================= */

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}
function html(body, status = 200) {
  return new Response(String(body ?? ""), {
    status,
    headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
  });
}
function cors(allow) {
  const o = String(allow || "*");
  return {
    "access-control-allow-origin": o,
    "access-control-allow-credentials": "true",
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "vary": "Origin",
  };
}
function jsonC(obj, status, allow) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...cors(allow), "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

function must(v, msg) { if (!v) throw new Error(msg); return v; }
function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function normYear(v) {
  const n = parseInt(String(v ?? "").trim(), 10);
  return Number.isFinite(n) && n >= 2000 && n <= 2100 ? n : null;
}
function clampInt(v, min, max, def) {
  const n = parseInt(String(v ?? "").trim(), 10);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, n));
}
function normEmail(v) {
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return "";
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s) ? s : "";
}
function toBool(v) {
  if (v === true) return true;
  if (v === false) return false;
  const s = String(v ?? "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "y";
}
function parseYears(v) {
  return String(v ?? "")
    .split(/[;, \n\r\t]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}
function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

async function sha256Hex(s) {
  const bytes = new TextEncoder().encode(String(s ?? ""));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");
}

function makeSession(env, payload) {
  const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
  const bodyJson = JSON.stringify({ ...payload, ts: Date.now() });
  const body = btoa(bodyJson).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  const sig = btoa(`${secret}:${body}`).slice(0, 32).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  return `${body}.${sig}`;
}
function readCookie(cookieHeader, name) {
  const m = String(cookieHeader || "").match(new RegExp("(^|;\\s*)" + name + "=([^;]+)"));
  return m ? m[2] : "";
}
function verifySession(env, token) {
  const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
  if (!token) return { ok: false };
  const parts = String(token).split(".");
  if (parts.length !== 2) return { ok: false };
  const body = parts[0];
  const sig = parts[1];
  const expect = btoa(`${secret}:${body}`).slice(0, 32).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  if (sig !== expect) return { ok: false };
  try {
    const b64 = body.replaceAll("-", "+").replaceAll("_", "/");
    const obj = JSON.parse(atob(b64));
    return { ok: true, email: obj.email, member_id: obj.member_id };
  } catch {
    return { ok: false };
  }
}

/**
 * RFC4180-ish CSV parser
 * - supports quoted fields with commas/newlines
 * - supports escaped quotes ("")
 * - returns rows as objects using header row
 */
function parseCsv(text) {
  const s = String(text ?? "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (!s.trim()) return [];

  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < s.length; i++) {
    const c = s[i];

    if (inQuotes) {
      if (c === '"') {
        const next = s[i + 1];
        if (next === '"') { field += '"'; i++; }
        else { inQuotes = false; }
      } else {
        field += c;
      }
      continue;
    }

    if (c === '"') { inQuotes = true; continue; }
    if (c === ",") { row.push(field); field = ""; continue; }
    if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; continue; }
    field += c;
  }
  row.push(field);
  rows.push(row);

  const clean = rows.filter((r) => r.some((v) => String(v ?? "").trim() !== ""));
  if (clean.length < 2) return [];

  const header = clean[0].map((x) => String(x ?? "").trim());
  const out = [];
  for (const cols of clean.slice(1)) {
    const obj = {};
    for (let i = 0; i < header.length; i++) {
      obj[header[i]] = String(cols[i] ?? "").trim();
    }
    out.push(obj);
  }
  return out;
}

// money -> cents (strong)
function parseMoneyToCents(v) {
  const raw = String(v ?? "").trim();
  if (!raw) return null;
  const neg = raw.startsWith("(") && raw.endsWith(")");
  const s0 = neg ? raw.slice(1, -1) : raw;
  let s = s0.replace(/[\s$¥]/g, "");
  s = s.replace(/,/g, "");
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  const cents = Math.round(n * 100);
  return neg ? -cents : cents;
}

function fmtName(first, last, email) {
  const name = [String(first || "").trim(), String(last || "").trim()].filter(Boolean).join(" ").trim();
  return name || String(email || "").trim() || "(unknown)";
}

/* ===================== HubSpot ===================== */

function hsHeaders(env) {
  const token = must(env.HUBSPOT_ACCESS_TOKEN, "Missing HUBSPOT_ACCESS_TOKEN");
  return { authorization: `Bearer ${token}`, "content-type": "application/json" };
}
async function hubspotGetContactByIdProperty(env, idValue, idProperty, properties = []) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(String(idValue))}`);
  u.searchParams.set("idProperty", String(idProperty));
  if (properties.length) u.searchParams.set("properties", properties.join(","));
  return fetch(u.toString(), { headers: hsHeaders(env) });
}
async function hubspotGetContactByEmail(env, email, properties = []) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(String(email))}`);
  u.searchParams.set("idProperty", "email");
  if (properties.length) u.searchParams.set("properties", properties.join(","));
  return fetch(u.toString(), { headers: hsHeaders(env) });
}
async function hubspotPatchContactByIdProperty(env, idValue, idProperty, properties) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(String(idValue))}`);
  u.searchParams.set("idProperty", String(idProperty));
  return fetch(u.toString(), {
    method: "PATCH",
    headers: hsHeaders(env),
    body: JSON.stringify({ properties: properties || {} }),
  });
}

/* ===================== Resend / Mail ===================== */

async function sendResend(env, { to, subject, html }) {
  const apiKey = must(env.RESEND_API_KEY, "Missing RESEND_API_KEY");
  const from = must(env.MAIL_FROM, "Missing MAIL_FROM");
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ from, to: [to], subject, html }),
  });
  if (!res.ok) throw new Error(`resend_failed: ${res.status} ${await res.text().catch(() => "")}`);
  return res;
}
function otpHtml(code) {
  return `<div style="font-family:Arial,sans-serif;line-height:1.6">
    <p>Your verification code is:</p>
    <p style="font-size:24px;font-weight:800;letter-spacing:2px">${escapeHtml(code)}</p>
    <p>This code will expire in 10 minutes.</p>
    <p>— World Divine Light</p>
  </div>`;
}

/* ===================== PDF ===================== */

async function getTemplateConfig(env) {
  try {
    const row = await env.RECEIPTS_DB.prepare(
      "SELECT page,name_x,name_y,year_x,year_y,amount_x,amount_y,date_x,date_y,font_size FROM receipt_template_config WHERE id=1"
    ).first();
    if (row) return row;
  } catch {}
  return { page: 0, name_x: 152, name_y: 650, year_x: 450, year_y: 650, amount_x: 410, amount_y: 548, date_x: 450, date_y: 520, font_size: 12 };
}

function drawRight(page, font, size, text, xRight, y) {
  const t = String(text ?? "");
  const w = font.widthOfTextAtSize(t, size);
  page.drawText(t, { x: Number(xRight) - w, y: Number(y), size, font, color: rgb(0, 0, 0) });
}

async function generateReceiptPdf(env, { name, year, amount, date }) {
  if (!_tmplCache) {
    const t = await env.RECEIPTS_BUCKET.get("templates/receipt_template_v1.pdf");
    if (!t) throw new Error("template_not_found");
    _tmplCache = await t.arrayBuffer();
  }
  if (!_fontCache) {
    const f = await env.RECEIPTS_BUCKET.get("templates/fonts/NotoSansJP-Regular.otf");
    if (!f) throw new Error("jp_font_not_found");
    _fontCache = await f.arrayBuffer();
  }

  const pdf = await PDFDocument.load(_tmplCache.slice(0));
  pdf.registerFontkit(fontkit);
  const jpFont = await pdf.embedFont(_fontCache, { subset: true });

  const cfg = await getTemplateConfig(env);
  const pages = pdf.getPages();
  const pageIndex = Math.max(0, Math.min(Number(cfg.page || 0), pages.length - 1));
  const page = pages[pageIndex];
  const size = Number(cfg.font_size || 12);

  page.drawText(String(name ?? ""), { x: Number(cfg.name_x), y: Number(cfg.name_y), size, font: jpFont, color: rgb(0, 0, 0) });

　page.drawText(String(year ?? ""), {
  x: Number(cfg.year_x), y: Number(cfg.year_y),
  size, font: jpFont, color: rgb(0,0,0)
});

page.drawText(String(amount ?? ""), {
  x: Number(cfg.amount_x), y: Number(cfg.amount_y),
  size, font: jpFont, color: rgb(0,0,0)
});

page.drawText(String(date ?? ""), {
  x: Number(cfg.date_x), y: Number(cfg.date_y),
  size, font: jpFont, color: rgb(0,0,0)
});
  
  return await pdf.save();
}

/* ===================== D1 ===================== */

async function upsertAnnual(env, row) {
  const year = row.year;
  const member_id = row.member_id;
  await env.RECEIPTS_DB.prepare(`
    INSERT INTO receipt_annual (year, member_id, branch, name, amount_cents, issue_date, pdf_key, status, error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(year, member_id) DO UPDATE SET
      branch=excluded.branch,
      name=excluded.name,
      amount_cents=excluded.amount_cents,
      issue_date=excluded.issue_date,
      pdf_key=excluded.pdf_key,
      status=excluded.status,
      error=excluded.error
  `).bind(
    year,
    member_id,
    row.branch,
    row.name,
    row.amount_cents,
    row.issue_date,
    row.pdf_key,
    row.status,
    row.error
  ).run();
}

/* ===================== Queues handlers ===================== */

async function handleParseCsv(env, job_id) {
  const job = await env.RECEIPTS_DB.prepare(`SELECT * FROM receipt_import_job WHERE job_id=?`).bind(job_id).first();
  if (!job) return;

  const csv_key = String(job.csv_key || `uploads/${job_id}.csv`);
  const csvObj = await env.RECEIPTS_BUCKET.get(csv_key);

  if (!csvObj) {
    try { await env.RECEIPTS_DB.prepare(`UPDATE receipt_import_job SET status='ERROR' WHERE job_id=?`).bind(job_id).run(); } catch {}
    return;
  }

  const csvText = await csvObj.text();
  const rows = parseCsv(csvText);
  const total = rows.length;

  try {
    await env.RECEIPTS_DB.prepare(`
      UPDATE receipt_import_job
      SET total_rows=?, next_index=0, processed_rows=0, ok_rows=0, ng_rows=0, status='RUNNING', updated_at=datetime('now')
      WHERE job_id=?
    `).bind(total, job_id).run();
  } catch {}

  try { await env.RECEIPTS_DB.prepare(`UPDATE receipt_import_job SET phase='PROCESSING' WHERE job_id=?`).bind(job_id).run(); } catch {}

  await env.RECEIPTS_DB.prepare(`DELETE FROM receipt_import_row WHERE job_id=?`).bind(job_id).run();

  const CHUNK = 200;
  for (let base = 0; base < total; base += CHUNK) {
    const slice = rows.slice(base, base + CHUNK);
    const stmts = slice.map((r, i) =>
      env.RECEIPTS_DB.prepare(`
        INSERT INTO receipt_import_row(job_id,row_index,member_id,branch,amount,year,status,updated_at)
        VALUES(?,?,?,?,?,?, 'PENDING', datetime('now'))
      `).bind(
        job_id,
        base + i,
        String(r.member_id || "").trim(),
        String(r.branch || "").trim(),
        String(r.amount || "").trim(),
        String(r.year || "").trim()
      )
    );
    await env.RECEIPTS_DB.batch(stmts);
  }

  await env.IMPORT_Q.send({ type: "process_rows", job_id });
}

async function handleProcessRows(env, job_id) {
  const job = await env.RECEIPTS_DB.prepare(`SELECT * FROM receipt_import_job WHERE job_id=?`).bind(job_id).first();
  if (!job) return;

  if (String(job.status || "").toUpperCase() === "DONE") return;

  const total = Number(job.total_rows || 0);
  let next_index = Number(job.next_index || 0);
  if (!Number.isFinite(next_index)) next_index = 0;

  // fetch next PENDING rows
  const BATCH = 3;
  let rowsRes = await env.RECEIPTS_DB.prepare(`
    SELECT * FROM receipt_import_row
    WHERE job_id=? AND row_index>=? AND status='PENDING'
    ORDER BY row_index
    LIMIT ?
  `).bind(job_id, next_index, BATCH).all();

  let list = rowsRes.results || [];

  if (!list.length) {
    const minPending = await env.RECEIPTS_DB.prepare(`
      SELECT MIN(row_index) AS m FROM receipt_import_row
      WHERE job_id=? AND status='PENDING'
    `).bind(job_id).first();

    const m = minPending?.m;
    if (m === null || m === undefined) {
      await env.RECEIPTS_DB.prepare(`
        UPDATE receipt_import_job SET status='DONE', next_index=?, processed_rows=?, updated_at=datetime('now')
        WHERE job_id=?
      `).bind(total, total, job_id).run();
      return;
    }

    next_index = Number(m);
    rowsRes = await env.RECEIPTS_DB.prepare(`
      SELECT * FROM receipt_import_row
      WHERE job_id=? AND row_index>=? AND status='PENDING'
      ORDER BY row_index
      LIMIT ?
    `).bind(job_id, next_index, BATCH).all();
    list = rowsRes.results || [];
    if (!list.length) return;
  }

  const t0 = Date.now();
  const TIME_LIMIT_MS = 2500;

  let ok = 0, ng = 0;
  let maxRowIndex = next_index;

  for (const r of list) {
    if (Date.now() - t0 > TIME_LIMIT_MS) break;

    const row_index = Number(r.row_index);
    maxRowIndex = Math.max(maxRowIndex, row_index + 1);

    const member_id = String(r.member_id || "").trim();
    const branch = String(r.branch || "").trim();
    const cents = parseMoneyToCents(r.amount);
    const year = normYear(r.year) || Number(job.year);

    if (!member_id || !branch || cents === null || !year) {
      ng++;
      await env.RECEIPTS_DB.prepare(`
        UPDATE receipt_import_row SET status='ERROR', error='invalid_row', updated_at=datetime('now')
        WHERE job_id=? AND row_index=?
      `).bind(job_id, row_index).run();
      continue;
    }

    const hs = await hubspotGetContactByIdProperty(env, member_id, "member_id",
      ["email","firstname","lastname","receipt_years_available"]
    ).catch(() => null);

    if (!hs || !hs.ok) {
      ng++;
      await env.RECEIPTS_DB.prepare(`
        UPDATE receipt_import_row SET status='ERROR', error='hubspot_not_found', updated_at=datetime('now')
        WHERE job_id=? AND row_index=?
      `).bind(job_id, row_index).run();
      continue;
    }

    const p = (await hs.json()).properties || {};
    const email = normEmail(p.email);
    const name = fmtName(p.firstname, p.lastname, email);

    if (!email) {
      ng++;

      await upsertAnnual(env, {
        year,
        member_id,
        branch,
        name,
        amount_cents: cents,
        issue_date: todayISO(),
        pdf_key: "",
        status: "ERROR",
        error: "missing_email",
      });

      await env.RECEIPTS_DB.prepare(`
        UPDATE receipt_import_row SET status='NEEDS_EMAIL', error='missing_email', updated_at=datetime('now')
        WHERE job_id=? AND row_index=?
      `).bind(job_id, row_index).run();

      continue;
    }

    // patch (best effort)
    try {
      const years = parseYears(p.receipt_years_available);
      if (!years.includes(String(year))) years.push(String(year));
      await hubspotPatchContactByIdProperty(env, member_id, "member_id", {
        receipt_portal_eligible: true,
        receipt_years_available: years.join(";"),
      });
    } catch {}

    const pdf_key = `receipts/${member_id}/${year}.pdf`;
    const pdf = await generateReceiptPdf(env, {
      name,
      year: String(year),
      amount: (cents / 100).toFixed(2),
      date: todayISO(),
    });
    await env.RECEIPTS_BUCKET.put(pdf_key, pdf, { httpMetadata: { contentType: "application/pdf" } });

    await upsertAnnual(env, {
      year,
      member_id,
      branch,
      name,
      amount_cents: cents,
      issue_date: todayISO(),
      pdf_key,
      status: "DONE",
      error: null,
    });

    await env.RECEIPTS_DB.prepare(`
      UPDATE receipt_import_row SET status='DONE', email=?, pdf_key=?, updated_at=datetime('now')
      WHERE job_id=? AND row_index=?
    `).bind(email, pdf_key, job_id, row_index).run();

    ok++;
  }

  const processed_rows = Math.min(total, maxRowIndex);

  await env.RECEIPTS_DB.prepare(`
    UPDATE receipt_import_job
    SET ok_rows=ok_rows+?,
        ng_rows=ng_rows+?,
        processed_rows=?,
        next_index=?,
        status='RUNNING',
        updated_at=datetime('now')
    WHERE job_id=?
  `).bind(ok, ng, processed_rows, maxRowIndex, job_id).run();

  const pending = await env.RECEIPTS_DB.prepare(`
    SELECT COUNT(*) AS n FROM receipt_import_row WHERE job_id=? AND status='PENDING'
  `).bind(job_id).first();

  const left = Number(pending?.n || 0);
  if (left > 0) {
    await env.IMPORT_Q.send({ type: "process_rows", job_id });
  } else {
    await env.RECEIPTS_DB.prepare(`
      UPDATE receipt_import_job SET status='DONE', next_index=?, processed_rows=?, updated_at=datetime('now')
      WHERE job_id=?
    `).bind(total, total, job_id).run();
  }
}

/* ===================== Member UI HTML ===================== */

function memberPortalHtml() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Receipt Portal</title>
  <style>
    :root{--b:#e5e7eb;--fg:#111827;--muted:#6b7280;--blue:#2563eb;}
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;padding:24px;color:var(--fg);background:#fff;line-height:1.6}
    .wrap{max-width:760px;margin:0 auto}
    .card{border:1px solid var(--b);border-radius:14px;padding:22px;background:#fff}
    h1{font-size:24px;margin:0 0 10px}
    .muted{color:var(--muted);font-size:13px}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;width:100%;margin-top:14px}
    .field{display:flex;flex-direction:column;width:100%}
    .field label{margin-bottom:6px;font-weight:600;font-size:13px}
    .field input{width:100%;box-sizing:border-box;padding:10px 12px;border:1px solid var(--b);border-radius:10px;font-size:14px}
    @media (max-width:640px){ .grid{grid-template-columns:1fr} }
    .row{display:flex;gap:12px;flex-wrap:wrap;margin-top:14px}
    button{padding:10px 14px;border-radius:10px;border:1px solid var(--b);background:#111827;color:#fff;cursor:pointer;font-weight:700}
    button.secondary{background:#fff;color:#111827}
    button:disabled{opacity:.5;cursor:not-allowed}
    .msg{margin-top:12px;padding:10px 12px;border:1px solid var(--b);border-radius:10px;background:#f9fafb;font-size:14px;white-space:pre-wrap;display:none}
    .years{margin-top:18px}
    .year-item{display:flex;align-items:center;justify-content:space-between;border:1px solid var(--b);border-radius:10px;padding:10px 12px;margin-top:10px}
    a{color:var(--blue)}
  </style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <h1>Receipt Portal</h1>
    <p>Please sign in with your email address and a one-time verification code.</p>
    <p class="muted">If you arrived here from the receipt email, use the same email address you received it at.</p>

    <div class="grid">
      <div class="field">
        <label>Email</label>
        <input id="email" type="email" placeholder="you@example.com" autocomplete="email" />
      </div>
      <div class="field">
        <label>Verification code</label>
        <input id="code" type="text" inputmode="numeric" placeholder="6 digits" maxlength="6" />
      </div>
    </div>

    <div class="row">
      <button id="btnSend">Send code</button>
      <button id="btnVerify" class="secondary">Verify</button>
      <button id="btnRefresh" class="secondary" disabled>Refresh years</button>
    </div>

    <div id="msg" class="msg"></div>

    <div class="years">
      <div style="font-weight:800;margin-top:18px">Available receipts</div>
      <div id="yearsList" class="muted">Not signed in yet.</div>
    </div>
  </div>
</div>

<script>
  const API = "https://api.kamikumite.worlddivinelight.org";
  const $ = (id) => document.getElementById(id);

  function showMsg(t){
    const el = $("msg");
    el.style.display = "block";
    el.textContent = t;
  }
  function clearMsg(){
    const el = $("msg");
    el.style.display = "none";
    el.textContent = "";
  }

  async function postJson(path, body){
    const r = await fetch(API + path, {
      method: "POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify(body || {}),
      credentials: "include"
    });
    const ct = r.headers.get("content-type") || "";
    const data = ct.includes("application/json") ? await r.json().catch(()=>null) : await r.text().catch(()=> "");
    return { r, data };
  }

  async function getJson(path){
    const r = await fetch(API + path, { method:"GET", credentials:"include" });
    const ct = r.headers.get("content-type") || "";
    const data = ct.includes("application/json") ? await r.json().catch(()=>null) : await r.text().catch(()=> "");
    return { r, data };
  }

  async function renderYears(){
    const box = $("yearsList");
    box.innerHTML = "";
    const { r, data } = await getJson("/api/receipt/me");
    if (!r.ok || !data || data.ok === false) {
      box.textContent = "Not signed in yet.";
      $("btnRefresh").disabled = true;
      return;
    }
    $("btnRefresh").disabled = false;

    const years = Array.isArray(data.years) ? data.years : [];
    if (!years.length) {
      box.textContent = "No receipts available.";
      return;
    }

    const list = document.createElement("div");
    for (const y of years) {
      const item = document.createElement("div");
      item.className = "year-item";
      const left = document.createElement("div");
      left.innerHTML = "<b>" + y + "</b>";
      const right = document.createElement("div");
      const a = document.createElement("a");
      a.href = API + "/api/receipt/pdf?year=" + encodeURIComponent(y);
      a.textContent = "Download PDF";
      a.target = "_blank";
      a.rel = "noopener";
      right.appendChild(a);
      item.appendChild(left);
      item.appendChild(right);
      list.appendChild(item);
    }
    box.appendChild(list);
  }

  $("btnSend").addEventListener("click", async () => {
    clearMsg();
    const email = ($("email").value || "").trim();
    if (!email) return showMsg("Please enter your email.");

    const { r, data } = await postJson("/api/receipt/request-code", { email });
    if (r.ok && data && data.ok) {
      showMsg("A verification code has been sent to your email.\\nPlease check your inbox.");
      return;
    }
    showMsg("Failed to send code:\\n" + JSON.stringify(data));
  });

  $("btnVerify").addEventListener("click", async () => {
    clearMsg();
    const email = ($("email").value || "").trim();
    const code = ($("code").value || "").trim();
    if (!email) return showMsg("Please enter your email.");
    if (!/^\\d{6}$/.test(code)) return showMsg("Please enter the 6-digit code.");

    const { r, data } = await postJson("/api/receipt/verify-code", { email, code });
    if (r.ok && data && data.ok) {
      showMsg("Signed in successfully.\\nAvailable years: " + (data.years || []).join(", "));
      await renderYears();
      return;
    }
    showMsg("Verification failed:\\n" + JSON.stringify(data));
  });

  $("btnRefresh").addEventListener("click", async () => {
    clearMsg();
    await renderYears();
  });

  renderYears().catch(()=>{});
</script>
</body>
</html>`;
}
