import { PDFDocument, StandardFonts, rgb } from "pdf-lib";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const host = url.hostname;
    const pathname = url.pathname;

    try {
      // =========================
      // ADMIN API (Access)
      // =========================
      if (pathname.startsWith("/api/admin/receipt/")) {
        // version
        if (pathname === "/api/admin/receipt/_version" && request.method === "GET") {
          return json({ ok: true, worker: "kamikumite-receipt", build: "CSV_IMPORT_v1+PDFGEN_v1" });
        }

        // ---------- TEMPLATE CONFIG ----------
        if (pathname === "/api/admin/receipt/template/config" && request.method === "GET") {
          const cfg = await getTemplateConfig(env);
          return json({ ok: true, config: cfg });
        }

        if (pathname === "/api/admin/receipt/template/config" && request.method === "POST") {
          const body = await request.json().catch(() => null);
          if (!body) return json({ ok: false, error: "invalid_json" }, 400);

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

          await upsertTemplateConfig(env, next);
          return json({ ok: true, config: next });
        }

        // ---------- TEMPLATE TEST PDF ----------
        if (pathname === "/api/admin/receipt/template/test.pdf" && request.method === "GET") {
          const name = (url.searchParams.get("name") || "John Doe").trim();
          const year = (url.searchParams.get("year") || String(new Date().getFullYear() - 1)).trim();
          const amount = (url.searchParams.get("amount") || "0").trim();
          const date = (url.searchParams.get("date") || new Date().toISOString().slice(0, 10)).trim();
          return await renderTemplatePdf(env, { name, year, amount, date, inline: true });
        }

        // =========================
        // CSV IMPORT (new)
        // =========================

        // POST /api/admin/receipt/import  (Content-Type: text/csv)
        if (pathname === "/api/admin/receipt/import" && request.method === "POST") {
          const contentType = request.headers.get("content-type") || "";
          if (!contentType.includes("text/csv") && !contentType.includes("application/csv") && !contentType.includes("text/plain")) {
            return json({ ok: false, error: "content_type_must_be_csv" }, 415);
          }
          if (!env.RECEIPTS_DB) return json({ ok: false, error: "D1_not_bound" }, 501);
          if (!env.RECEIPTS_BUCKET) return json({ ok: false, error: "R2_not_bound" }, 501);

          const csvText = await request.text();
          const rows = parseCsvRows(csvText);
          if (rows.length === 0) return json({ ok: false, error: "no_rows" }, 400);

          const jobId = crypto.randomUUID();
          const year = normalizeYear(rows[0].year) ?? (new Date().getFullYear() - 1);

          await env.RECEIPTS_DB.prepare(`
            INSERT INTO receipt_import_job (job_id, year, total_rows, processed_rows, ok_rows, ng_rows, next_index, status)
            VALUES (?, ?, ?, 0, 0, 0, 0, 'READY')
          `).bind(jobId, year, rows.length).run();

          await env.RECEIPTS_BUCKET.put(`uploads/${jobId}.csv`, csvText, { httpMetadata: { contentType: "text/csv" } });

          return json({ ok: true, job_id: jobId, year, total_rows: rows.length });
        }

        // POST /api/admin/receipt/import/continue?job_id=...&batch=20
        if (pathname === "/api/admin/receipt/import/continue" && request.method === "POST") {
          const jobId = String(url.searchParams.get("job_id") || "").trim();
          const batch = clampInt(url.searchParams.get("batch"), 1, 100, 20);
          if (!jobId) return json({ ok: false, error: "job_id_required" }, 400);

          const job = await getJob(env, jobId);
          if (!job) return json({ ok: false, error: "job_not_found" }, 404);

          const csvObj = await env.RECEIPTS_BUCKET.get(`uploads/${jobId}.csv`);
          if (!csvObj) return json({ ok: false, error: "uploaded_csv_not_found" }, 404);

          const csvText = await csvObj.text();
          const rows = parseCsvRows(csvText);

          const start = job.next_index;
          const end = Math.min(rows.length, start + batch);

          await setJobStatus(env, jobId, "RUNNING", null);

          let okCount = 0;
          let ngCount = 0;

          for (let i = start; i < end; i++) {
            const r = rows[i];
            const memberId = String(r.member_id || "").trim();
            const branch = String(r.branch || "").trim();
            const amountCents = parseAmountToCents(r.amount);
            const year = normalizeYear(r.year) ?? job.year;

            if (!memberId || !branch || amountCents === null) {
              ngCount++;
              await upsertAnnual(env, { year, member_id: memberId, branch, name: "(invalid)", amount_cents: 0, issue_date: todayISO(), pdf_key: "", status: "ERROR", error: "invalid_row" });
              continue;
            }

            // HubSpot lookup by member_id (idProperty=member_id)
            const hs = await hubspotGetContactByIdProperty(env, memberId, "member_id", ["firstname","lastname","email","member_id","receipt_years_available","receipt_portal_eligible"]);
            if (hs.status === 404 || !hs.ok) {
              ngCount++;
              await upsertAnnual(env, { year, member_id: memberId, branch, name: "(hubspot not found)", amount_cents: amountCents, issue_date: todayISO(), pdf_key: "", status: "ERROR", error: "hubspot_not_found" });
              continue;
            }

            const contact = await hs.json();
            const p = contact.properties || {};
            const name = formatName(p.firstname, p.lastname, p.email);

            // Update HubSpot: eligible true + add year
            const years = parseYears(String(p.receipt_years_available || "").trim());
            if (!years.includes(String(year))) years.push(String(year));
            const yearsStr = years.join(";");

            const patch = await hubspotPatchContactByIdProperty(env, memberId, "member_id", {
              receipt_portal_eligible: true,
              receipt_years_available: yearsStr
            });

            if (!patch.ok) {
              ngCount++;
              await upsertAnnual(env, { year, member_id: memberId, branch, name, amount_cents: amountCents, issue_date: todayISO(), pdf_key: "", status: "ERROR", error: "hubspot_patch_failed" });
              continue;
            }

            // Generate PDF and store to R2
            const pdfKey = `receipts/${memberId}/${year}.pdf`;
            const pdfBytes = await generateReceiptPdf(env, {
              name,
              year: String(year),
              amount: formatUsd(amountCents),
              date: todayISO()
            });

            await env.RECEIPTS_BUCKET.put(pdfKey, pdfBytes, { httpMetadata: { contentType: "application/pdf" } });

            await upsertAnnual(env, { year, member_id: memberId, branch, name, amount_cents: amountCents, issue_date: todayISO(), pdf_key: pdfKey, status: "DONE", error: null });
            okCount++;
          }

          const processed = end;
          await env.RECEIPTS_DB.prepare(`
            UPDATE receipt_import_job
            SET processed_rows = ?, ok_rows = ok_rows + ?, ng_rows = ng_rows + ?, next_index = ?, updated_at = datetime('now'),
                status = CASE WHEN ? >= total_rows THEN 'DONE' ELSE status END
            WHERE job_id = ?
          `).bind(processed, okCount, ngCount, end, end, jobId).run();

          const updated = await getJob(env, jobId);
          return json({ ok: true, job: updated, batch: { start, end, ok: okCount, ng: ngCount }, done: updated.status === "DONE" });
        }

        // GET /api/admin/receipt/import/status?job_id=...
        if (pathname === "/api/admin/receipt/import/status" && request.method === "GET") {
          const jobId = String(url.searchParams.get("job_id") || "").trim();
          if (!jobId) return json({ ok: false, error: "job_id_required" }, 400);
          const job = await getJob(env, jobId);
          if (!job) return json({ ok: false, error: "job_not_found" }, 404);
          return json({ ok: true, job });
        }

        // GET /api/admin/receipt/dashboard?year=2025
        if (pathname === "/api/admin/receipt/dashboard" && request.method === "GET") {
          const year = normalizeYear(url.searchParams.get("year")) ?? (new Date().getFullYear() - 1);
          const rows = await env.RECEIPTS_DB.prepare(`
            SELECT year, member_id, branch, name, amount_cents, issue_date, pdf_key, status, error
            FROM receipt_annual
            WHERE year = ?
            ORDER BY branch ASC, name ASC
          `).bind(year).all();
          return json({ ok: true, year, rows: rows.results || [] });
        }

        return json({ ok: false, error: "Not found" }, 404);
      }

      return json({ ok: false, error: "Not found" }, 404);
    } catch (e) {
      return json({ ok: false, error: "server_error", detail: String(e?.stack || e) }, 500);
    }
  },
};

// ---------- PDF helpers ----------
async function renderTemplatePdf(env, { name, year, amount, date, inline }) {
  if (!env.RECEIPTS_BUCKET) return json({ ok: false, error: "R2_not_bound" }, 501);
  const obj = await env.RECEIPTS_BUCKET.get("templates/receipt_template_v1.pdf");
  if (!obj) return json({ ok: false, error: "template_not_found", key: "templates/receipt_template_v1.pdf" }, 404);

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
      "content-disposition": `${inline ? "inline" : "attachment"}; filename="template_test.pdf"`
    }
  });
}

async function generateReceiptPdf(env, { name, year, amount, date }) {
  if (!env.RECEIPTS_BUCKET) throw new Error("R2_not_bound");
  const obj = await env.RECEIPTS_BUCKET.get("templates/receipt_template_v1.pdf");
  if (!obj) throw new Error("template_not_found");

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

  return await pdf.save();
}

// ---------- D1 helpers ----------
async function getTemplateConfig(env) {
  const row = await env.RECEIPTS_DB.prepare(
    "SELECT page,name_x,name_y,year_x,year_y,amount_x,amount_y,date_x,date_y,font_size FROM receipt_template_config WHERE id=1"
  ).first();

  return row || {
    page: 0,
    name_x: 152, name_y: 650,
    year_x: 450, year_y: 650,
    amount_x: 410, amount_y: 548,
    date_x: 450, date_y: 520,
    font_size: 12
  };
}
async function upsertTemplateConfig(env, cfg) {
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
    cfg.page,
    cfg.name_x, cfg.name_y,
    cfg.year_x, cfg.year_y,
    cfg.amount_x, cfg.amount_y,
    cfg.date_x, cfg.date_y,
    cfg.font_size
  ).run();
}
async function upsertAnnual(env, row) {
  await env.RECEIPTS_DB.prepare(`
    INSERT INTO receipt_annual
      (year, member_id, branch, name, amount_cents, issue_date, pdf_key, status, error, updated_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(year, member_id) DO UPDATE SET
      branch=excluded.branch,
      name=excluded.name,
      amount_cents=excluded.amount_cents,
      issue_date=excluded.issue_date,
      pdf_key=excluded.pdf_key,
      status=excluded.status,
      error=excluded.error,
      updated_at=datetime('now')
  `).bind(
    row.year, row.member_id, row.branch, row.name, row.amount_cents, row.issue_date, row.pdf_key, row.status, row.error
  ).run();
}
async function getJob(env, jobId) {
  return await env.RECEIPTS_DB.prepare(
    "SELECT job_id, year, total_rows, processed_rows, ok_rows, ng_rows, next_index, status, last_error, created_at, updated_at FROM receipt_import_job WHERE job_id=?"
  ).bind(jobId).first();
}
async function setJobStatus(env, jobId, status, lastError) {
  await env.RECEIPTS_DB.prepare(
    "UPDATE receipt_import_job SET status=?, last_error=?, updated_at=datetime('now') WHERE job_id=?"
  ).bind(status, lastError, jobId).run();
}

// ---------- CSV parser ----------
function parseCsvRows(text) {
  const lines = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n").filter(l => l.trim());
  if (lines.length < 2) return [];
  const header = lines[0].split(",").map(s => s.trim().toLowerCase());
  const idx = (name) => header.indexOf(name);

  const iMember = idx("member_id");
  const iBranch = idx("branch");
  const iAmount = idx("amount");
  const iYear = idx("year");

  if (iMember < 0 || iBranch < 0 || iAmount < 0) return [];

  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(",").map(s => s.trim());
    out.push({
      member_id: cols[iMember] || "",
      branch: cols[iBranch] || "",
      amount: cols[iAmount] || "",
      year: (iYear >= 0 ? (cols[iYear] || "") : "")
    });
  }
  return out;
}
function parseAmountToCents(v) {
  const s = String(v || "").replace(/[$,]/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100);
}
function formatUsd(cents) { return (Number(cents) / 100).toFixed(2); }
function todayISO() { return new Date().toISOString().slice(0, 10); }
function normalizeYear(v) {
  const n = Number.parseInt(String(v || ""), 10);
  if (Number.isFinite(n) && n >= 2000 && n <= 2100) return n;
  return null;
}
function clampInt(v, min, max, def) {
  const n = Number.parseInt(String(v || ""), 10);
  if (Number.isNaN(n)) return def;
  return Math.max(min, Math.min(max, n));
}

// ---------- Shared ----------
function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}
async function safeText(res) { try { return await res.text(); } catch { return ""; } }
function must(v, msg) { if (!v) throw new Error(msg); return v; }
function parseYears(s) {
  const norm = String(s || "").trim().replace(/\s+/g, "").replace(/,/g, ";").replace(/;;+/g, ";").replace(/;$/g, "");
  if (!norm) return [];
  return norm.split(";").filter(Boolean);
}
function toBool(v) {
  if (typeof v === "boolean") return v;
  const s = String(v || "").toLowerCase().trim();
  return s === "true" || s === "1" || s === "yes";
}

// ---------- HubSpot ----------
function hsHeaders(env) {
  const token = must(env.HUBSPOT_ACCESS_TOKEN, "Missing HUBSPOT_ACCESS_TOKEN");
  return { authorization: `Bearer ${token}`, "content-type": "application/json" };
}
async function hubspotGetContactByIdProperty(env, idValue, idProperty, properties) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(idValue)}`);
  u.searchParams.set("idProperty", idProperty);
  if (properties?.length) u.searchParams.set("properties", properties.join(","));
  return fetch(u.toString(), { headers: hsHeaders(env) });
}
async function hubspotPatchContactByIdProperty(env, idValue, idProperty, properties) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(idValue)}`);
  u.searchParams.set("idProperty", idProperty);
  return fetch(u.toString(), { method: "PATCH", headers: hsHeaders(env), body: JSON.stringify({ properties }) });
}
async function hubspotGetContactByEmail(env, email, properties) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(email)}`);
  u.searchParams.set("idProperty", "email");
  if (properties?.length) u.searchParams.set("properties", properties.join(","));
  return fetch(u.toString(), { headers: hsHeaders(env) });
}
function formatName(firstname, lastname, email) {
  const fn = String(firstname || "").trim();
  const ln = String(lastname || "").trim();
  const full = (fn + " " + ln).trim();
  if (full) return full;
  const e = String(email || "").trim();
  const local = e.includes("@") ? e.split("@")[0] : "Member";
  return local || "Member";
}

// ---------- Resend ----------
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
function otpEmailHtml({ code }) {
  return `<div style="font-family:Arial,sans-serif;line-height:1.6">
    <p>Your verification code is:</p>
    <p style="font-size:24px;font-weight:800;letter-spacing:2px">${escapeHtml(code)}</p>
    <p>This code will expire in 10 minutes.</p>
    <p>— World Divine Light</p>
  </div>`;
}
function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c])
  );
}

// ---------- Member UI placeholder ----------
function memberPortalHtml(env) {
  return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Receipt Portal</title></head><body style="font-family:system-ui;padding:24px">
  <h2>Receipt Portal</h2><p>OK</p></body></html>`;
}
