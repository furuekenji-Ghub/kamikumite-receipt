export interface Env {
  HUBSPOT_ACCESS_TOKEN: string;
}

type ValidateError =
  | { type: "MISSING_MEMBER_ID"; row: number }
  | { type: "MISSING_EMAIL"; row: number; member_id: string; name: string };

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
  });
}

function methodNotAllowed(): Response {
  return json({ ok: false, error: "Method Not Allowed" }, 405);
}

function notFound(): Response {
  return json({ ok: false, error: "Not Found" }, 404);
}

function parseCsvSimple(csvText: string): { header: string[]; rows: string[][] } {
  const lines = csvText
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .filter((l) => l.trim() !== "");

  if (lines.length === 0) return { header: [], rows: [] };

  const parseLine = (line: string) => {
    const out: string[] = [];
    let cur = "";
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQ && line[i + 1] === '"') {
          cur += '"';
          i++;
          continue;
        }
        inQ = !inQ;
        continue;
      }
      if (ch === "," && !inQ) {
        out.push(cur);
        cur = "";
        continue;
      }
      cur += ch;
    }
    out.push(cur);
    return out.map((s) => s.trim());
  };

  const header = parseLine(lines[0]).map((h) => h.trim().toLowerCase());
  const rows = lines.slice(1).map(parseLine);
  return { header, rows };
}

async function hubspotFindByMemberId(env: Env, memberId: string): Promise<{ name: string; email: string }> {
  const url = "https://api.hubapi.com/crm/v3/objects/contacts/search";

  // member_id property name must match your HubSpot contact property
  const body = {
    filterGroups: [
      {
        filters: [{ propertyName: "member_id", operator: "EQ", value: memberId }],
      },
    ],
    properties: ["firstname", "lastname", "email", "member_id"],
    limit: 1,
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.HUBSPOT_ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`HubSpot search failed: ${res.status} ${txt}`);
  }

  const js: any = await res.json();
  const r = js?.results?.[0] ?? null;
  const props = r?.properties ?? {};
  const email = String(props.email ?? "").trim();
  const first = String(props.firstname ?? "").trim();
  const last = String(props.lastname ?? "").trim();
  const name = `${first} ${last}`.trim() || "(no name)";
  return { name, email };
}

async function handleImportValidate(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return methodNotAllowed();

  const csvText = await request.text();
  const { header, rows } = parseCsvSimple(csvText);

  const midIdx = header.indexOf("member_id");
  if (midIdx < 0) {
    // header missing is effectively "missing member id"
    return json({ ok: false, errors: [{ type: "MISSING_MEMBER_ID", row: 1 }] }, 200);
  }

  const errors: ValidateError[] = [];

  // 1) missing member_id rows
  for (let i = 0; i < rows.length; i++) {
    const rowNum = i + 2; // header row is 1
    const memberId = String(rows[i][midIdx] ?? "").trim();
    if (!memberId) errors.push({ type: "MISSING_MEMBER_ID", row: rowNum });
  }

  // block if missing member_id exists
  if (errors.some((e) => e.type === "MISSING_MEMBER_ID")) {
    return json({ ok: false, errors }, 200);
  }

  // 2) hubspot email check for unique IDs
  const unique = new Map<string, number[]>();
  for (let i = 0; i < rows.length; i++) {
    const rowNum = i + 2;
    const memberId = String(rows[i][midIdx] ?? "").trim();
    if (!unique.has(memberId)) unique.set(memberId, []);
    unique.get(memberId)!.push(rowNum);
  }

  for (const [memberId, rowNums] of unique.entries()) {
    const hs = await hubspotFindByMemberId(env, memberId);
    if (!hs.email) {
      errors.push({ type: "MISSING_EMAIL", row: rowNums[0], member_id: memberId, name: hs.name });
    }
  }

  if (errors.length > 0) {
    return json({ ok: false, errors }, 200);
  }

  return json({ ok: true, summary: { rows: rows.length, unique_ids: unique.size } }, 200);
}

/**
 * ✅ IMPORTANT:
 * Replace these placeholders with your existing implementations.
 * (You already have them in current Worker.)
 */
async function handleVersion(_req: Request): Promise<Response> {
  // TODO: replace with your existing /_version handler
  return json({ ok: true, worker: "kamikumite-receipt", build: "UNKNOWN" });
}
async function handleDashboard(_req: Request): Promise<Response> {
  // TODO: replace with your existing /dashboard handler
  return json({ ok: true, rows: [] });
}
async function handleImport(_req: Request): Promise<Response> {
  // TODO: replace with your existing /import handler
  return json({ ok: true, job_id: "TODO" });
}
async function handleImportContinue(_req: Request): Promise<Response> {
  // TODO: replace with your existing /import/continue handler
  return json({ ok: true });
}
async function handleRebuild(_req: Request): Promise<Response> {
  // TODO: replace with your existing /rebuild handler
  return json({ ok: true });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // ✅ Admin Receipt routes (as you defined)
    // /api/admin/receipt/*
    if (path === "/api/admin/receipt/_version") return handleVersion(request);

    if (path === "/api/admin/receipt/dashboard") return handleDashboard(request);

    if (path === "/api/admin/receipt/import" && request.method === "POST") return handleImport(request);

    if (path === "/api/admin/receipt/import/continue" && request.method === "POST") return handleImportContinue(request);

    // ✅ NEW: validate endpoint
    if (path === "/api/admin/receipt/import/validate") return handleImportValidate(request, env);

    if (path === "/api/admin/receipt/rebuild" && request.method === "POST") return handleRebuild(request);

    return notFound();
  },
};
