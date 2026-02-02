import { PDFDocument, StandardFonts, rgb } from "pdf-lib";

/**
 * Receipt Worker — CSV_IMPORT + PDFGEN + MAIL + REBUILD + VALIDATE
 * CSV quotes + comma OK, no routing collision version
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const host = url.hostname;
    const path = url.pathname;

    try {
      /* =====================================================
       * MEMBER UI
       * ===================================================== */
      if (host === "kamikumite.worlddivinelight.org" && path.startsWith("/receipt")) {
        return html(memberPortalHtml(), 200);
      }

      /* =====================================================
       * ADMIN API (GUARD)
       * ===================================================== */
      if (path.startsWith("/api/admin/receipt/")) {

        

        // --- version ---
        if (path === "/api/admin/receipt/_version" && request.method === "GET") {
          return json({
            ok: true,
            worker: "kamikumite-receipt",
            build: "CSV_IMPORT+PDFGEN+MAIL+REBUILD+VALIDATE_v3_CSV_QUOTES_OK"
          });
        }

// =====================================================
// Email Bulk Job (START / STATUS / CONTINUE)
// =====================================================

// POST /api/admin/receipt/email/job/start
if (path === "/api/admin/receipt/email/job/start" && request.method === "POST") {
  const body = await request.json().catch(() => null);
  const year = normYear(body?.year);
  const branch = String(body?.branch || "ALL").trim();
  const mode = String(body?.mode || "unsent").trim().toLowerCase(); // unsent | all
  if (!year) return json({ ok: false, error: "year_required" }, 400);

  const job_id = crypto.randomUUID();

  let q = `SELECT COUNT(*) as n FROM receipt_annual WHERE year=? AND status='DONE'`;
  const args = [year];
  if (branch !== "ALL") { q += ` AND branch=?`; args.push(branch); }
  if (mode === "unsent") { q += ` AND (email_sent_at IS NULL OR TRIM(email_sent_at)='')`; }

  const cnt = await env.RECEIPTS_DB.prepare(q).bind(...args).first();
  const total_targets = Number(cnt?.n || 0);

  await env.RECEIPTS_DB.prepare(`
    INSERT INTO receipt_email_job(job_id, year, branch, total_targets, next_index, sent_ok, sent_ng, status, created_at, updated_at)
    VALUES(?, ?, ?, ?, 0, 0, 0, 'READY', datetime('now'), datetime('now'))
  `).bind(job_id, year, branch, total_targets).run();

  return json({ ok: true, job_id, year, branch, mode, total_targets });
}

// GET /api/admin/receipt/email/job/status?job_id=...
if (path === "/api/admin/receipt/email/job/status" && request.method === "GET") {
  const job_id = String(url.searchParams.get("job_id") || "").trim();
  if (!job_id) return json({ ok: false, error: "job_id_required" }, 400);

  const job = await env.RECEIPTS_DB.prepare(`SELECT * FROM receipt_email_job WHERE job_id=?`).bind(job_id).first();
  if (!job) return json({ ok: false, error: "job_not_found" }, 404);

  return json({ ok: true, job });
}

// POST /api/admin/receipt/email/job/continue?job_id=...&batch=25&mode=unsent
if (path === "/api/admin/receipt/email/job/continue" && request.method === "POST") {
  const job_id = String(url.searchParams.get("job_id") || "").trim();
  const batch = clampInt(url.searchParams.get("batch"), 1, 100, 25);
  const mode = String(url.searchParams.get("mode") || "unsent").trim().toLowerCase(); // unsent | all
  if (!job_id) return json({ ok: false, error: "job_id_required" }, 400);

  const job = await env.RECEIPTS_DB.prepare(`SELECT * FROM receipt_email_job WHERE job_id=?`).bind(job_id).first();
  if (!job) return json({ ok: false, error: "job_not_found" }, 404);

  const st = String(job.status || "").toUpperCase();
  if (st === "DONE") return json({ ok: true, done: true, job });
  if (st === "CANCELED") return json({ ok: false, error: "job_canceled", job }, 409);

  const year = Number(job.year);
  const branch = String(job.branch || "ALL");
  const start = Number(job.next_index || 0);

  let q = `
    SELECT year, member_id, branch, name, amount_cents, email, status, email_sent_at
    FROM receipt_annual
    WHERE year=? AND status='DONE'
  `;
  const args = [year];

  if (branch !== "ALL") { q += ` AND branch=?`; args.push(branch); }
  if (mode === "unsent") { q += ` AND (email_sent_at IS NULL OR TRIM(email_sent_at)='')`; }

  q += ` ORDER BY branch, name, member_id LIMIT ? OFFSET ?`;
  args.push(batch, start);

  const rows = await env.RECEIPTS_DB.prepare(q).bind(...args).all();
  const list = rows.results || [];

  let sent_ok = 0, sent_ng = 0;
  const results = [];

  for (const r of list) {
    const member_id = String(r.member_id || "").trim();
    const rowYear = Number(r.year);
    let email = String(r.email || "").trim().toLowerCase();

    if (!email) {
  try {
    const hs = await hubspotGetContactByIdProperty(env, member_id, "member_id", ["email"]);
    if (hs && hs.ok) {
      const p = (await hs.json()).properties || {};
      email = String(p.email || "").trim().toLowerCase();
    }
  } catch {
    email = "";
  }
}

    if (!email) {
      sent_ng++;
      results.push({ member_id, ok: false, error: "missing_email" });

      await env.RECEIPTS_DB.prepare(`
        UPDATE receipt_annual
        SET email_status='NEEDS_EMAIL', email_error='missing_email'
        WHERE year=? AND member_id=?
      `).bind(rowYear, member_id).run();
      continue;
    }

    try {
      await sendReceiptNoticeEmail(env, {
        to: email,
        name: r.name || "Member",
        year: rowYear,
        amount_cents: Number(r.amount_cents || 0),
      });

      sent_ok++;
      results.push({ member_id, ok: true, to: email });

      await env.RECEIPTS_DB.prepare(`
        UPDATE receipt_annual
        SET email=?, email_status='SENT', email_error=NULL, email_sent_at=datetime('now')
        WHERE year=? AND member_id=?
      `).bind(email, rowYear, member_id).run();

    } catch (e) {
      const msg = String(e?.message || e);
      sent_ng++;
      results.push({ member_id, ok: false, to: email, error: msg });

      await env.RECEIPTS_DB.prepare(`
        UPDATE receipt_annual
        SET email=?, email_status='FAILED', email_error=?
        WHERE year=? AND member_id=?
      `).bind(email, msg, rowYear, member_id).run();
    }
  }

  const next_index = start + list.length;
  const done = list.length < batch;

  await env.RECEIPTS_DB.prepare(`
    UPDATE receipt_email_job
    SET next_index=?,
        sent_ok=sent_ok+?,
        sent_ng=sent_ng+?,
        status=?,
        updated_at=datetime('now')
    WHERE job_id=?
  `).bind(next_index, sent_ok, sent_ng, done ? "DONE" : "RUNNING", job_id).run();

  const updated = await env.RECEIPTS_DB.prepare(`SELECT * FROM receipt_email_job WHERE job_id=?`).bind(job_id).first();

  return json({
    ok: true,
    done,
    job: updated,
    batch: { start, count: list.length, sent_ok, sent_ng, next_index },
    results
  });
}

        // --- email/test ---
        if (path === "/api/admin/receipt/email/test" && request.method === "POST") {
          if (String(env.EMAIL_TEST_ENABLED || "").toLowerCase() !== "true") {
          return json({ ok: false, error: "not_found" }, 404);
          }
          const body = await request.json().catch(() => null);

          const to = String(body?.to || "").trim().toLowerCase();
          const name = String(body?.name || "Test User").trim();
          const year = normYear(body?.year) || new Date().getFullYear();
          const amount_cents = Number.isFinite(body?.amount_cents)
            ? Number(body.amount_cents)
            : 100;
          const dry_run = body?.dry_run === true;

          if (!to) return json({ ok: false, error: "to_required" }, 400);
          if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(to)) {
            return json({ ok: false, error: "to_invalid" }, 400);
          }

          if (dry_run) {
            return json({ ok: true, sent: false, dry_run: true, to, year, amount_cents });
          }

          try {
            const r = await sendReceiptNoticeEmail(env, { to, name, year, amount_cents });

            let provider_status = null;
            let provider_body = null;
            if (r && typeof r === "object" && typeof r.status === "number") {
              provider_status = r.status;
              provider_body = await r.text().catch(() => null);
            }

            const sent = provider_status
              ? provider_status >= 200 && provider_status < 300
              : true;

            return json({
              ok: true,
              sent,
              to,
              year,
              amount_cents,
              provider_status,
              provider_body
            });
          } catch (e) {
            return json({
              ok: true,
              sent: false,
              to,
              year,
              amount_cents,
              warning: String(e?.message || e)
            });
          }
        }

        // --- hubspot email debug ---
if (path === "/api/admin/receipt/hubspot/email" && request.method === "GET") {
  const member_id = String(url.searchParams.get("member_id") || "").trim();
  if (!member_id) return json({ ok:false, error:"member_id_required" }, 400);

  try {
    const hs = await hubspotGetContactByIdProperty(
      env,
      member_id,
      "member_id",
      ["email","firstname","lastname"]
    );

    const status = hs.status;
    const text = await hs.text().catch(() => "");
    let email = "";

    try {
      const parsed = JSON.parse(text);
      email = String(parsed?.properties?.email || "").trim().toLowerCase();
    } catch {}

    return json({
      ok: true,
      member_id,
      hubspot_status: status,
      email,
      raw: text.slice(0, 500)
    });
  } catch (e) {
    return json({
      ok: false,
      error: "hubspot_exception",
      detail: String(e?.message || e)
    }, 200);
  }
}

        // --- import/validate ---
        if (path === "/api/admin/receipt/import/validate" && request.method === "POST") {
          const csvText = await request.text();
          const rows = parseCsv(csvText);
          if (!rows.length) return json({ ok: false, error: "no_rows" }, 400);

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

            const hs = await hubspotGetContactByIdProperty(
              env,
              member_id,
              "member_id",
              ["email", "firstname", "lastname"]
            );
            if (hs.status === 404 || !hs.ok) {
              errors.push({ type: "HUBSPOT_NOT_FOUND", row: rowNo, member_id });
              continue;
            }

            const p = (await hs.json()).properties || {};
            if (!normEmail(p.email)) {
              errors.push({
                type: "MISSING_EMAIL",
                row: rowNo,
                member_id,
                name: fmtName(p.firstname, p.lastname, p.email)
              });
            }
          }

          if (errors.length) {
            return json({ ok: false, errors, checked_rows: checkRows.length, total_rows: rows.length });
          }
          return json({ ok: true, checked_rows: checkRows.length, total_rows: rows.length });
        }

        // --- dashboard ---
        if (path === "/api/admin/receipt/dashboard" && request.method === "GET") {
          const year = normYear(url.searchParams.get("year")) ?? (new Date().getFullYear() - 1);
          const rows = await env.RECEIPTS_DB.prepare(`
            SELECT year, member_id, branch, name, amount_cents, issue_date, pdf_key, status, error
            FROM receipt_annual
            WHERE year=?
            ORDER BY branch, name
          `).bind(year).all();

          return json({ ok: true, year, rows: rows.results || [] });
        }

        // --- pdf view ---
        if (path === "/api/admin/receipt/pdf" && request.method === "GET") {
          const member_id = String(url.searchParams.get("member_id") || "").trim();
          const year = String(url.searchParams.get("year") || "").trim();
          if (!member_id || !/^\d{4}$/.test(year)) {
            return json({ ok: false, error: "member_id_and_year_required" }, 400);
          }

          const key = `receipts/${member_id}/${year}.pdf`;
          const obj = await env.RECEIPTS_BUCKET.get(key);
          if (!obj) return json({ ok: false, error: "pdf_not_found", key }, 404);

          return new Response(obj.body, {
            status: 200,
            headers: {
              "content-type": "application/pdf",
              "cache-control": "no-store",
              "content-disposition": "inline"
            }
          });
        }

        // --- import ---
        if (path === "/api/admin/receipt/import" && request.method === "POST") {
          const csvText = await request.text();
          const rows = parseCsv(csvText);
          if (!rows.length) return json({ ok: false, error: "no_rows" }, 400);

          const job_id = crypto.randomUUID();
          const year = normYear(rows[0].year) ?? (new Date().getFullYear() - 1);

          await env.RECEIPTS_DB.prepare(`
            INSERT INTO receipt_import_job
            (job_id,year,total_rows,processed_rows,ok_rows,ng_rows,next_index,status,created_at,updated_at)
            VALUES(?,?,?,0,0,0,0,'READY',datetime('now'),datetime('now'))
          `).bind(job_id, year, rows.length).run();

          await env.RECEIPTS_BUCKET.put(`uploads/${job_id}.csv`, csvText, {
            httpMetadata: { contentType: "text/csv" }
          });

          return json({ ok: true, job_id, year, total_rows: rows.length });
        }

        // --- import/continue ---
        if (path === "/api/admin/receipt/import/continue" && request.method === "POST") {
          const job_id = String(url.searchParams.get("job_id") || "").trim();
          const batch = clampInt(url.searchParams.get("batch"), 1, 200, 50);
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
            const cents = parseMoneyToCents(r.amount);

            if (!r.member_id || !r.branch || cents === null || !year) {
              ng++;
              await upsertAnnual(env, {
                year: year || job.year,
                member_id: r.member_id || "(missing)",
                branch: r.branch || "(missing)",
                name: "(invalid)",
                amount_cents: 0,
                issue_date: todayISO(),
                pdf_key: "",
                status: "ERROR",
                error: "invalid_row"
              });
              continue;
            }

            const hs = await hubspotGetContactByIdProperty(
              env,
              r.member_id,
              "member_id",
              ["firstname", "lastname", "email", "receipt_years_available"]
            );
            if (!hs.ok) {
              ng++;
              await upsertAnnual(env, {
                year,
                member_id: r.member_id,
                branch: r.branch,
                name: "(HubSpot not found)",
                amount_cents: cents,
                issue_date: todayISO(),
                pdf_key: "",
                status: "ERROR",
                error: "hubspot_not_found"
              });
              continue;
            }

            const p = (await hs.json()).properties || {};
            const email = normEmail(p.email);
            if (!email) {
              ng++;
              await upsertAnnual(env, {
                year,
                member_id: r.member_id,
                branch: r.branch,
                name: fmtName(p.firstname, p.lastname, p.email),
                amount_cents: cents,
                issue_date: todayISO(),
                pdf_key: "",
                status: "ERROR",
                error: "missing_email"
              });
              continue;
            }

            const name = fmtName(p.firstname, p.lastname, p.email);
            const years = parseYears(p.receipt_years_available);
            if (!years.includes(String(year))) years.push(String(year));

            await hubspotPatchContactByIdProperty(env, r.member_id, "member_id", {
              receipt_portal_eligible: true,
              receipt_years_available: years.join(";")
            });

            const pdf_key = `receipts/${r.member_id}/${year}.pdf`;
            const pdf = await generateReceiptPdf(env, {
              name,
              year: String(year),
              amount: (cents / 100).toFixed(2),
              date: todayISO()
            });

            await env.RECEIPTS_BUCKET.put(pdf_key, pdf, {
              httpMetadata: { contentType: "application/pdf" }
            });

            await upsertAnnual(env, {
              year,
              member_id: r.member_id,
              branch: r.branch,
              name,
              amount_cents: cents,
              issue_date: todayISO(),
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

        // --- admin fallback ---
        return json({ ok: false, error: "not_found" }, 404);
      }

      /* =====================================================
       * MEMBER API (UNCHANGED BEHAVIOR)
       * ===================================================== */
      if (host === "api.kamikumite.worlddivinelight.org" && path.startsWith("/api/receipt/")) {
        const allow = env.PORTAL_ORIGIN || "https://kamikumite.worlddivinelight.org";
        const origin = request.headers.get("Origin") || "";
        if (request.method === "OPTIONS") {
          return new Response(null, { status: 204, headers: cors(allow, origin) });
        }

        // ---- request-code ----
        if (path === "/api/receipt/request-code" && request.method === "POST") {
          if (!env.OTP_KV) return jsonC({ ok: false, error: "kv_not_bound" }, 501, allow, origin);

          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allow, origin);

          const hs = await hubspotGetContactByEmail(
            env,
            email,
            ["member_id", "receipt_portal_eligible", "receipt_years_available"]
          );
          if (hs.status === 404) return jsonC({ ok: false, error: "not_registered" }, 404, allow, origin);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed", detail: await safeText(hs) }, 500, allow, origin);

          const p = (await hs.json()).properties || {};
          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allow, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(p.receipt_years_available);
          if (!memberId || !years.length) return jsonC({ ok: false, error: "not_ready" }, 403, allow, origin);

          const code = String(Math.floor(100000 + Math.random() * 900000));
          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
          const hash = await sha256Hex(`${code}:${secret}`);

          await env.OTP_KV.put(
            `otp:${email}`,
            JSON.stringify({
              hash,
              member_id: memberId,
              years,
              exp: Date.now() + 10 * 60_000,
              attempts: 0
            }),
            { expirationTtl: 10 * 60 }
          );

          await sendResend(env, {
            to: email,
            subject: "Your verification code for the Receipt Portal",
            html: otpHtml(code)
          });

          return jsonC({ ok: true }, 200, allow, origin);
        }

        // ---- verify-code ----
        if (path === "/api/receipt/verify-code" && request.method === "POST") {
          const body = await request.json().catch(() => null);
          const email = normEmail(body?.email);
          const code = String(body?.code || "").trim();
          if (!email) return jsonC({ ok: false, error: "email_required" }, 400, allow, origin);
          if (!/^\d{6}$/.test(code)) return jsonC({ ok: false, error: "code_invalid" }, 400, allow, origin);

          const rec = await env.OTP_KV.get(`otp:${email}`, "json");
          if (!rec) return jsonC({ ok: false, error: "expired" }, 401, allow, origin);
          if (Date.now() > Number(rec.exp || 0)) return jsonC({ ok: false, error: "expired" }, 401, allow, origin);

          const secret = must(env.SESSION_SECRET || env.MAGICLINK_SECRET, "Missing SESSION_SECRET");
          const hash = await sha256Hex(`${code}:${secret}`);
          if (hash !== rec.hash) return jsonC({ ok: false, error: "wrong" }, 401, allow, origin);

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

        // ---- me ----
        if (path === "/api/receipt/me" && request.method === "GET") {
          const s = verifySession(env, readCookie(request.headers.get("Cookie") || "", "receipt_session"));
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allow, origin);

          const hs = await hubspotGetContactByEmail(env, s.email, ["receipt_years_available"]);
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow, origin);

          const years = parseYears((await hs.json()).properties?.receipt_years_available);
          return jsonC({ ok: true, years }, 200, allow, origin);
        }

        // ---- pdf ----
        if (path === "/api/receipt/pdf" && request.method === "GET") {
          const s = verifySession(env, readCookie(request.headers.get("Cookie") || "", "receipt_session"));
          if (!s.ok) return jsonC({ ok: false, error: "not_logged_in" }, 401, allow, origin);

          const year = String(url.searchParams.get("year") || "").trim();
          if (!/^\d{4}$/.test(year)) return jsonC({ ok: false, error: "invalid_year" }, 400, allow, origin);

          const hs = await hubspotGetContactByEmail(
            env,
            s.email,
            ["member_id", "receipt_years_available", "receipt_portal_eligible"]
          );
          if (!hs.ok) return jsonC({ ok: false, error: "hubspot_get_failed" }, 500, allow, origin);

          const p = (await hs.json()).properties || {};
          if (!toBool(p.receipt_portal_eligible)) return jsonC({ ok: false, error: "not_eligible" }, 403, allow, origin);

          const memberId = String(p.member_id || "").trim();
          const years = parseYears(p.receipt_years_available);
          if (!memberId || !years.includes(year)) return jsonC({ ok: false, error: "year_not_available" }, 403, allow, origin);

          const key = `receipts/${memberId}/${year}.pdf`;
          const obj = await env.RECEIPTS_BUCKET.get(key);
          if (!obj) return jsonC({ ok: false, error: "pdf_not_found", key }, 404, allow, origin);

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

        // --- member fallback ---
        return jsonC({ ok: false, error: "not_found" }, 404, allow, origin);
      }

      // ===================== DEFAULT =====================
      return json({ ok: false, error: "not_found" }, 404);

    } catch (e) {
      return json({ ok: false, error: "server_error", detail: String(e?.stack || e) }, 500);
    }
  }
};

/* =========================================================
 * Helper functions (unchanged)
 * ========================================================= */

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
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

/* ===================== HubSpot (required) ===================== */

function hsHeaders(env) {
  const token = must(env.HUBSPOT_ACCESS_TOKEN, "Missing HUBSPOT_ACCESS_TOKEN");
  return {
    "authorization": `Bearer ${token}`,
    "content-type": "application/json"
  };
}

// GET contact by arbitrary idProperty (e.g. member_id)
async function hubspotGetContactByIdProperty(env, idValue, idProperty, properties = []) {
  const u = new URL(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(String(idValue))}`);
  u.searchParams.set("idProperty", String(idProperty));
  if (properties.length) u.searchParams.set("properties", properties.join(","));
  return fetch(u.toString(), { headers: hsHeaders(env) });
}

/* ===================== Resend / Mail (required) ===================== */

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
    headers: {
      "Authorization": `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from,
      to: [to],
      subject,
      html,
    }),
  });

  if (!res.ok) {
    throw new Error(`resend_failed: ${res.status} ${await res.text().catch(() => "")}`);
  }

  return res;
}

/* ===================== Small helpers (required) ===================== */

function must(v, msg) {
  if (!v) throw new Error(msg);
  return v;
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[c]));
}

// ... (helpers: sendReceiptNoticeEmail, sendResend, HubSpot, PDF, CSV, utils)
// Keep ALL your existing helper implementations below this line unchanged
