import { PDFDocument, StandardFonts, rgb } from "pdf-lib";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const host = url.hostname;
    const pathname = url.pathname;

    try {
      // =========================================================
      // MEMBER UI (English only)
      // Route: kamikumite.worlddivinelight.org/receipt*
      // =========================================================
      if (host === "kamikumite.worlddivinelight.org" && pathname.startsWith("/receipt")) {
        return new Response(memberPortalHtml(env), {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
        });
      }

      // =========================================================
      // MEMBER API (OTP + me + pdf)
      // Route: api.kamikumite.worlddivinelight.org/api/receipt/*
      // =========================================================
      if (host === "api.kamikumite.worlddivinelight.org" && pathname.startsWith("/api/receipt/")) {
        const allowOrigin = env.PORTAL_ORIGIN || "https://kamikumite.worlddivinelight.org";
        const origin = request.headers.get("Origin") || "";

        if (request.method === "OPTIONS") {
          return new Response(null, { status: 204, headers: corsHeaders(allowOrigin, origin) });
        }

        // POST /api/receipt/request-code
        if (pathname === "/api/receipt/request-code" && request.method === "POST") {
          if (!env.OTP_KV) return jsonC({ ok: false, error: "kv_not_bound" }, 501, allowOrigin, origin);

          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allowOrigin, origin);

          // rate limit 60s
          const rlKey = `rl:${email}`;
          const rl = await env.OTP_KV.get(rlKey, "json");
          const now = Date.now();
          const last = Number(rl?.lastSendTs || 0);
          if (now - last < 60_000) return jsonC({ ok: false, error: "rate_limited" }, 429, allowOrigin, origin);

          // eligibility by email
          const hs = await hubspotGetContactByEmail(env, email, ["email","member_id","receipt_portal_eligible","receipt_years_available"]);
          if (hs.status === 404) return jsonC({ ok: false, error: "not_registered" }, 404, allowOrigin, origin);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed", detail: await safeText(hs) }, hs.status, allowOrigin, origin);

          const contact = await hs.json();
          const p = contact.properties || {};
          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allowOrigin, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(String(p.receipt_years_available || "").trim());
          if (!memberId) return jsonC({ ok: false, error: "member_id_missing" }, 403, allowOrigin, origin);
          if (!years.length) return jsonC({ ok: false, error: "no_years" }, 403, allowOrigin, origin);

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
            html: otpEmailHtml({ code })
          });

          return jsonC({ ok: true, sent: true }, 200, allowOrigin, origin);
        }

        // POST /api/receipt/verify-code
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

          return new Response(JSON.stringify({ ok: true, years: record.years || [] }), {
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

          const hs = await hubspotGetContactByEmail(env, s.email, ["receipt_years_available"]);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allowOrigin, origin);
          const contact = await hs.json();
          const years = parseYears(String(contact.properties?.receipt_years_available || "").trim());
          return jsonC({ ok: true, years }, 200, allowOrigin, origin);
        }

        // GET /api/receipt/pdf?year=2025
        if (pathname === "/api/receipt/pdf" && request.method === "GET") {
          const sess = readCookie(request.headers.get("Cookie") || "", "receipt_session");
          const s = verifySession(env, sess);
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allowOrigin, origin);

          const year = String(url.searchParams.get("year") || "").trim();
          if (!/^\d{4}$/.test(year)) return jsonC({ ok: false, error: "invalid_year" }, 400, allowOrigin, origin);

          const hs = await hubspotGetContactByEmail(env, s.email, ["member_id","receipt_years_available","receipt_portal_eligible"]);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allowOrigin, origin);
          const p = (await hs.json()).properties || {};

          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allowOrigin, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(String(p.receipt_years_available || "").trim());
          if (!memberId || !years.includes(year)) return jsonC({ ok: false, error: "year_not_available" }, 403, allowOrigin, origin);

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
              "content-disposition": `attachment; filename="receipt_${memberId}_${year}.pdf"`
            }
          });
        }

        return jsonC({ ok: false, error: "not_found" }, 404, allowOrigin, origin);
      }

      // =========================================================
      // ADMIN API (Access protected)
      // =========================================================
      if (!pathname.startsWith("/api/admin/receipt/")) {
        return json({ ok: false, error: "Not found" }, 404);
      }

      // version
      if (pathname === "/api/admin/receipt/_version" && request.method === "GET") {
        return json({ ok: true, worker: "kamikumite-receipt", build: "CSV_IMPORT_v1+PDFGEN_v1+ADMIN_UI_v1" });
      }

      // ---- Admin PDF download (NEW) ----
      // GET /api/admin/receipt/pdf?member_id=TEST-0001&year=2025
      if (pathname === "/api/admin/receipt/pdf" && request.method === "GET") {
        const memberId = String(url.searchParams.get("member_id") || "").trim();
        const year = String(url.searchParams.get("year") || "").trim();
        if (!memberId || !/^\d{4}$/.test(year)) return json({ ok: false, error: "member_id_and_year_required" }, 400);
        if (!env.RECEIPTS_BUCKET) return json({ ok: false, error: "r2_not_bound" }, 501);

        const key = `receipts/${memberId}/${year}.pdf`;
        const obj = await env.RECEIPTS_BUCKET.get(key);
        if (!obj) return json({ ok: false, error: "pdf_not_found", key }, 404);

        return new Response(obj.body, {
          status: 200,
          headers: {
            "content-type": "application/pdf",
            "cache-control": "no-store",
            "content-disposition": `inline; filename="receipt_${memberId}_${year}.pdf"`
          }
        });
      }

      // ---- template config ----
      if (pathname === "/api/admin/receipt/template/config" && request.method === "GET") {
        const cfg = await getTemplateConfig(env);
        return json({ ok: true, config: cfg });
      }

      if (pathname === "/api/admin/receipt/template/test.pdf" && request.method === "GET") {
        const name = (url.searchParams.get("name") || "John Doe").trim();
        const year = (url.searchParams.get("year") || String(new Date().getFullYear() - 1)).trim();
        const amount = (url.searchParams.get("amount") || "0").trim();
        const date = (url.searchParams.get("date") || new Date().toISOString().slice(0, 10)).trim();
        return await renderTemplatePdf(env, { name, year, amount, date, inline: true });
      }

      // =========================================================
      // ✅ NEW: PRE-CHECK API (validate CSV before import)
      // POST /api/admin/receipt/import/validate
      // Content-Type: text/csv
      // =========================================================
      if (pathname === "/api/admin/receipt/import/validate" && request.method === "POST") {
        const contentType = request.headers.get("content-type") || "";
        if (!contentType.includes("text/csv") && !contentType.includes("application/csv") && !contentType.includes("text/plain")) {
          return json({ ok: false, error: "content_type_must_be_csv" }, 415);
        }
        if (!env.HUBSPOT_ACCESS_TOKEN) return json({ ok: false, error: "Missing HUBSPOT_ACCESS_TOKEN" }, 500);

        const csvText = await request.text();
        const { header, rows, rawLineCount } = parseCsvRowsWithMeta(csvText);

        // header must include member_id
        const iMember = header.indexOf("member_id");
        if (iMember < 0) {
          return json({
            ok: false,
            errors: [{ type: "MISSING_MEMBER_ID", row: 1 }]
          }, 200);
        }

        const errors = [];

        // 1) missing member_id
        for (let i = 0; i < rows.length; i++) {
          const rowNum = i + 2; // header=1
          const memberId = String(rows[i][iMember] || "").trim();
          if (!memberId) errors.push({ type: "MISSING_MEMBER_ID", row: rowNum });
        }

        // block immediately if missing member_id exists (your requirement)
        if (errors.some(e => e.type === "MISSING_MEMBER_ID")) {
          return json({ ok: false, errors, summary: { rows: rows.length, lines: rawLineCount } }, 200);
        }

        // 2) check hubspot: email must exist for each unique member_id
        const seen = new Map(); // member_id -> firstRow
        for (let i = 0; i < rows.length; i++) {
          const rowNum = i + 2;
          const memberId = String(rows[i][iMember] || "").trim();
          if (!seen.has(memberId)) seen.set(memberId, rowNum);
        }

        for (const [memberId, rowNum] of seen.entries()) {
          // Use your existing helper: hubspotGetContactByIdProperty (idProperty=member_id)
          const hs = await hubspotGetContactByIdProperty(env, memberId, "member_id", ["firstname","lastname","email","member_id"]);
          if (hs.status === 404) {
            // Not explicitly requested, but safest to block: treat as missing email with name info
            errors.push({ type: "MISSING_EMAIL", row: rowNum, member_id: memberId, name: "(HubSpot not found)" });
            continue;
          }
          if (!hs.ok) {
            errors.push({ type: "MISSING_EMAIL", row: rowNum, member_id: memberId, name: "(HubSpot lookup failed)" });
            continue;
          }

          const contact = await hs.json();
          const p = contact.properties || {};
          const email = String(p.email || "").trim();
          const name = formatName(p.firstname, p.lastname, p.email);

          if (!email) {
            errors.push({ type: "MISSING_EMAIL", row: rowNum, member_id: memberId, name });
          }
        }

        if (errors.length) {
          return json({ ok: false, errors, summary: { rows: rows.length, unique_ids: seen.size, lines: rawLineCount } }, 200);
        }

        return json({ ok: true, summary: { rows: rows.length, unique_ids: seen.size, lines: rawLineCount } }, 200);
      }

      // POST import (text/csv)
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

      // POST continue
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

          const hs = await hubspotGetContactByIdProperty(env, memberId, "member_id", ["firstname","lastname","email","member_id","receipt_years_available","receipt_portal_eligible"]);
          if (hs.status === 404 || !hs.ok) {
            ngCount++;
            await upsertAnnual(env, { year, member_id: memberId, branch, name: "(hubspot not found)", amount_cents: amountCents, issue_date: todayISO(), pdf_key: "", status: "ERROR", error: "hubspot_not_found" });
            continue;
          }

          const contact = await hs.json();
          const p = contact.properties || {};
          const name = formatName(p.firstname, p.lastname, p.email);

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

      // dashboard
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

    } catch (e) {
      return json({ ok: false, error: "server_error", detail: String(e?.stack || e) }, 500);
    }
  },
};

// ---------- PDF helpers ----------
async function renderTemplatePdf(env, { name, year, amount, date, inline }) {
  if (!env.RECEIPTS_BUCKET) return json({ ok: false, error: "R2_not_bound" }, 501);
  const obj = await env.RECEIPTS_BUCKET.get("templates/receipt_template_v1.pdf");
  if (!obj) return json({ ok: false, error: "template_not_found" }, 404);

  const cfg = await getTemplateConfig(env);
  const bytes = await obj.arrayBuffer();
  const pdf = await PDFDocument.load(bytes);
  const pages = pdf.getPages();
  const page = pages[Math.max(0, Math.min(cfg.page, pages.length - 1))];

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
  const obj = await env.RECEIPTS_BUCKET.get("templates/receipt_template_v1.pdf");
  const cfg = await getTemplateConfig(env);
  const bytes = await obj.arrayBuffer();

  const pdf = await PDFDocument.load(bytes);
  const page = pdf.getPages()[Math.max(0, Math.min(cfg.page, pdf.getPages().length - 1))];

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
  return row || { page:0, name_x:152, name_y:650, year_x:450, year_y:650, amount_x:410, amount_y:548, date_x:450, date_y:520, font_size:12 };
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
  return lines.slice(1).map(line => {
    const cols = line.split(",").map(s => s.trim());
    return { member_id: cols[iMember] || "", branch: cols[iBranch] || "", amount: cols[iAmount] || "", year: (iYear>=0 ? (cols[iYear]||"") : "") };
  });
}

/**
 * ✅ NEW: validate 用メタ情報付きパーサ
 * - ヘッダ配列（lowercase）
 * - 行配列（cols[]）
 * - rawLineCount（空行除外の行数）
 */
function parseCsvRowsWithMeta(text) {
  const lines = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n").filter(l => l.trim());
  const rawLineCount = lines.length;
  if (lines.length < 2) return { header: [], rows: [], rawLineCount };

  const header = lines[0].split(",").map(s => s.trim().toLowerCase());
  const rows = lines.slice(1).map(line => line.split(",").map(s => s.trim()));
  return { header, rows, rawLineCount };
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
function must(v, msg) { if (!v) throw new Error(msg); return v; }
async function safeText(res) { try { return await res.text(); } catch { return ""; } }
function normalizeYears(s) { return String(s).trim().replace(/\s+/g,"").replace(/,/g,";").replace(/;;+/g,";").replace(/;$/g,""); }
function parseYears(s) { const norm = normalizeYears(s); return norm ? norm.split(";").filter(Boolean) : []; }
function toBool(v) { if (typeof v === "boolean") return v; const s = String(v || "").toLowerCase().trim(); return s==="true"||s==="1"||s==="yes"; }

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

// ---------- Member UI ----------
function memberPortalHtml(env) {
  return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Receipt Portal</title></head><body style="font-family:system-ui;padding:24px">
  <h2>Receipt Portal</h2><p>OK</p></body></html>`;
}
