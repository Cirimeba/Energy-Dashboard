const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 8080);
const INFLUX_URL = (process.env.INFLUX_URL || "").replace(/\/$/, "");
const INFLUX_ORG = process.env.INFLUX_ORG || "botlabs";
const INFLUX_BUCKET = process.env.INFLUX_BUCKET || "energia";
const INFLUX_TOKEN = process.env.INFLUX_TOKEN || "";
const ADMIN_USER = process.env.ADMIN_USER || "admin";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "admin";
const VIEWER_USER = process.env.VIEWER_USER || "viewer";
const VIEWER_PASSWORD = process.env.VIEWER_PASSWORD || "viewer";
const SESSION_SECRET = process.env.SESSION_SECRET || INFLUX_TOKEN || "change-this-session-secret";
const ROOT = __dirname;
const DASHBOARD = JSON.parse(fs.readFileSync(path.join(ROOT, "grafana-dashboard.json"), "utf8"));

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".css": "text/css; charset=utf-8"
};

function csvLine(line) {
  const row = [];
  let value = "";
  let quoted = false;
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (quoted && line[i + 1] === '"') { value += '"'; i++; }
      else quoted = !quoted;
    } else if (char === "," && !quoted) {
      row.push(value);
      value = "";
    } else value += char;
  }
  row.push(value);
  return row;
}

function parseAnnotatedCsv(text) {
  const lines = text.split(/\r?\n/).filter(line => line && !line.startsWith("#"));
  const tables = [];
  let header = null;
  let rows = [];
  for (const line of lines) {
    if (line.startsWith("result,") || header === null) {
      if (header && rows.length) tables.push(rows);
      header = csvLine(line);
      rows = [];
      continue;
    }
    const values = csvLine(line);
    if (values.length !== header.length) continue;
    rows.push(Object.fromEntries(header.map((key, index) => [key, values[index]])));
  }
  if (header && rows.length) tables.push(rows);
  return tables.flat();
}

function substituteQuery(query, start, stop, window) {
  return query
    .replace(/v\.timeRangeStart/g, start || "-24h")
    .replace(/v\.timeRangeStop/g, stop || "now()")
    .replace(/v\.windowPeriod/g, window || "1m")
    .replace(/v\.window/g, window || "1m");
}

async function executeQuery(query) {
  if (!INFLUX_URL || !INFLUX_TOKEN) throw new Error("InfluxDB is not configured");
  const response = await fetch(`${INFLUX_URL}/api/v2/query?org=${encodeURIComponent(INFLUX_ORG)}`, {
    method: "POST",
    headers: {
      Authorization: `Token ${INFLUX_TOKEN}`,
      "Content-Type": "application/vnd.flux",
      Accept: "application/csv"
    },
    body: query
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`InfluxDB ${response.status}: ${text.slice(0, 300)}`);
  if (!text.trim()) return [];
  return parseAnnotatedCsv(text).filter(row => row._time && row._value !== "");
}

const panels = new Map(DASHBOARD.panels.map(panel => [String(panel.id), panel]));

async function panelData(id, params) {
  const panel = panels.get(String(id));
  if (!panel) return { error: "Panel not found" };
  const targets = [];
  for (const target of panel.targets || []) {
    if (!target.query) continue;
    const query = substituteQuery(target.query, params.start, params.stop, params.window);
    try {
      targets.push({ refId: target.refId, data: await executeQuery(query) });
    } catch (error) {
      targets.push({ refId: target.refId, error: error.message });
    }
  }
  return { panelId: panel.id, title: panel.title, targets };
}

async function actualKelloggDemand() {
  const demandQuery = [
    `from(bucket: "${INFLUX_BUCKET}")`,
    '|> range(start: -24h)',
    '|> filter(fn: (r) => r["_measurement"] == "energia_periodo")',
    '|> filter(fn: (r) => r["planta"] == "queretaro")',
    '|> filter(fn: (r) => r["_field"] == "u_kw")',
    '|> aggregateWindow(every: 5m, fn: max, createEmpty: false, timeSrc: "_start")',
    '|> group()',
    '|> map(fn: (r) => ({ _time: r._time, _value: r._value, serie: "Demanda CFE" }))',
    '|> group(columns: ["serie"])',
    '|> last()'
  ].join("\n");
  const generationQuery = [
    `from(bucket: "${INFLUX_BUCKET}")`,
    '|> range(start: -24h)',
    '|> filter(fn: (r) => r["_measurement"] == "energia_periodo")',
    '|> filter(fn: (r) => r["planta"] == "queretaro")',
    '|> filter(fn: (r) => r["_field"] == "gen_kw")',
    '|> aggregateWindow(every: 5m, fn: mean, createEmpty: false, timeSrc: "_start")',
    '|> group()',
    '|> map(fn: (r) => ({ _time: r._time, _value: r._value, serie: "Cogeneración" }))',
    '|> group(columns: ["serie"])',
    '|> last()'
  ].join("\n");
  const gasQuery = [
    `from(bucket: "${INFLUX_BUCKET}")`,
    '|> range(start: -24h)',
    '|> filter(fn: (r) => r["_measurement"] == "colector_energia")',
    '|> filter(fn: (r) => r["host"] == "ThinkStation")',
    '|> filter(fn: (r) => r["_field"] == "SQLDB_GN_nm3")',
    '|> sort(columns: ["_time"])',
    '|> difference(nonNegative: true)',
    '|> aggregateWindow(every: 5m, fn: sum, createEmpty: false, timeSrc: "_start")',
    '|> last()'
  ].join("\n");
  const steamQuery = [
    `from(bucket: "${INFLUX_BUCKET}")`,
    '|> range(start: -24h)',
    '|> filter(fn: (r) => r["_measurement"] == "colector_energia")',
    '|> filter(fn: (r) => r["host"] == "ThinkStation")',
    '|> filter(fn: (r) => r["_field"] == "SQLDB_VaporCD_Kgh")',
    '|> aggregateWindow(every: 5m, fn: mean, createEmpty: false, timeSrc: "_start")',
    '|> last()'
  ].join("\n");

  const demandRows = await executeQuery(demandQuery);
  const generationRows = await executeQuery(generationQuery);
  const gasRows = await executeQuery(gasQuery);
  const steamRows = await executeQuery(steamQuery);
  const demand = demandRows[demandRows.length - 1];
  const generation = generationRows[generationRows.length - 1];
  const gas = gasRows[gasRows.length - 1];
  const steam = steamRows[steamRows.length - 1];

  const value = demand ? Number(demand._value) : NaN;
  const cogenerationValue = generation ? Number(generation._value) : NaN;
  const gasValue = gas ? Number(gas._value) : NaN;
  const steamValue = steam ? Number(steam._value) : NaN;

  return {
    value: Number.isFinite(value) ? value : null,
    unit: "kW",
    time: demand ? demand._time : null,
    cogenerationValue: Number.isFinite(cogenerationValue) ? cogenerationValue : null,
    cogenerationUnit: "kW",
    cogenerationTime: generation ? generation._time : null,
    gasValue: Number.isFinite(gasValue) ? gasValue : null,
    gasUnit: "Nm³",
    gasTime: gas ? gas._time : null,
    steamValue: Number.isFinite(steamValue) ? steamValue : null,
    steamUnit: "kg/h",
    steamTime: steam ? steam._time : null
  };
}

async function kelloggElectricalHistory(rangeStart, rangeStop, aggregateEvery = "5m") {
  const historyStart = rangeStart || "-24h";
  const historyStop = rangeStop || "now()";
  const cfeQuery = [
    `from(bucket: "${INFLUX_BUCKET}")`,
    `|> range(start: ${historyStart}, stop: ${historyStop})`,
    '|> filter(fn: (r) => r["_measurement"] == "energia_periodo")',
    '|> filter(fn: (r) => r["planta"] == "queretaro")',
    '|> filter(fn: (r) => r["_field"] == "u_kw")',
    `|> aggregateWindow(every: ${aggregateEvery}, fn: max, createEmpty: false, timeSrc: "_start")`,
    '|> group()',
  ].join("\n");
  const generationQuery = [
    `from(bucket: "${INFLUX_BUCKET}")`,
    `|> range(start: ${historyStart}, stop: ${historyStop})`,
    '|> filter(fn: (r) => r["_measurement"] == "energia_periodo")',
    '|> filter(fn: (r) => r["planta"] == "queretaro")',
    '|> filter(fn: (r) => r["_field"] == "gen_kw")',
    `|> aggregateWindow(every: ${aggregateEvery}, fn: mean, createEmpty: false, timeSrc: "_start")`,
    '|> group()',
  ].join("\n");
  const [cfeRows, generationRows] = await Promise.all([
    executeQuery(cfeQuery),
    executeQuery(generationQuery)
  ]);
  const byTime = new Map();
  for (const row of cfeRows) {
    const time = row._time;
    if (!byTime.has(time)) byTime.set(time, { time, u_kw: null, gen_kw: null });
    const value = Number(row._value);
    if (Number.isFinite(value)) byTime.get(time).u_kw = value;
  }
  for (const row of generationRows) {
    const time = row._time;
    if (!byTime.has(time)) byTime.set(time, { time, u_kw: null, gen_kw: null });
    const value = Number(row._value);
    if (Number.isFinite(value)) byTime.get(time).gen_kw = value;
  }
  return [...byTime.values()]
    .sort((a, b) => new Date(a.time) - new Date(b.time))
    .filter(point => point.u_kw !== null || point.gen_kw !== null)
    .map(point => ({
      time: point.time,
      cfeKw: point.u_kw,
      generationKw: point.gen_kw,
      totalKw: point.u_kw !== null && point.gen_kw !== null ? point.u_kw + point.gen_kw : null
    }));
}

function readCookies(req) {
    return Object.fromEntries((req.headers.cookie || "").split(";").map(part => {
      const index = part.indexOf("=");
      return index === -1 ? ["", ""] : [part.slice(0, index).trim(), decodeURIComponent(part.slice(index + 1).trim())];
    }).filter(([key]) => key));
  }

function sessionToken(user, role) {
    const payload = Buffer.from(JSON.stringify({ user, role, exp: Date.now() + 12 * 60 * 60 * 1000 })).toString("base64url");
    const signature = crypto.createHmac("sha256", SESSION_SECRET).update(payload).digest("base64url");
    return `${payload}.${signature}`;
  }

function readSession(req) {
    const token = readCookies(req).ems_session;
    if (!token) return null;
    const [payload, signature] = token.split(".");
    if (!payload || !signature) return null;
    const expected = crypto.createHmac("sha256", SESSION_SECRET).update(payload).digest("base64url");
    const signatureBuffer = Buffer.from(signature);
    const expectedBuffer = Buffer.from(expected);
    if (signatureBuffer.length !== expectedBuffer.length || !crypto.timingSafeEqual(signatureBuffer, expectedBuffer)) return null;
    try {
      const session = JSON.parse(Buffer.from(payload, "base64url").toString());
      return session.exp > Date.now() ? session : null;
    } catch (_) {
      return null;
    }
  }

function json(res, status, value, extraHeaders = {}) {
    res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store", ...extraHeaders });
    res.end(JSON.stringify(value));
  }

async function readJsonBody(req) {
    let body = "";
    for await (const chunk of req) body += chunk;
    return JSON.parse(body || "{}");
  }

function requireSession(req, res, adminOnly = false) {
    const session = readSession(req);
    if (!session || (adminOnly && session.role !== "admin")) {
      json(res, 401, { error: "Unauthorized" });
      return null;
    }
  return session;
}

function staticFile(req, res) {
    const pathname = decodeURIComponent(req.url.split("?")[0]);
    const relative = pathname === "/" ? "index.html" : pathname.replace(/^\/+/, "");
    const filename = path.resolve(ROOT, relative);
    if (!filename.startsWith(ROOT + path.sep)) {
      res.writeHead(403).end();
      return;
    }
  fs.readFile(filename, (error, data) => {
    if (error) { res.writeHead(404).end("Not found"); return; }
    res.writeHead(200, { "Content-Type": TYPES[path.extname(filename)] || "application/octet-stream", "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0" });
    res.end(data);
  });
}

http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname === "/api/auth/login" && req.method === "POST") {
    try {
      const { username, password } = await readJsonBody(req);
      let role = null;
      if (username === ADMIN_USER && password === ADMIN_PASSWORD) role = "admin";
      else if (username === VIEWER_USER && password === VIEWER_PASSWORD) role = "viewer";
      if (!role) return json(res, 401, { error: "Usuario o contraseña incorrectos" });
      const token = sessionToken(username, role);
      return json(res, 200, { user: username, role }, { "Set-Cookie": `ems_session=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=43200${process.env.NODE_ENV === "production" ? "; Secure" : ""}` });
    } catch (_) {
      return json(res, 400, { error: "Solicitud inválida" });
    }
  }
  if (url.pathname === "/api/auth/me") {
    const session = readSession(req);
    return json(res, session ? 200 : 401, session ? { user: session.user, role: session.role } : { error: "Unauthorized" });
  }
  if (url.pathname === "/api/auth/logout" && req.method === "POST") {
    return json(res, 200, { ok: true }, { "Set-Cookie": "ems_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0" });
  }
  if (url.pathname === "/api/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", influxConfigured: Boolean(INFLUX_URL && INFLUX_TOKEN) }));
    return;
  }
  if (url.pathname === "/api/actual") {
    if (!requireSession(req, res)) return;
    try {
      const data = await actualKelloggDemand();
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache", "Expires": "0" });
      res.end(JSON.stringify(data === null ? { value: null } : data));
    } catch (error) {
      res.writeHead(500, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }
  if (url.pathname === "/api/kellogg/electrical-history") {
    if (!requireSession(req, res)) return;
    try {
      const range = url.searchParams.get("range");
      const aggregate = range === "7d" ? "15m" : range === "30d" || range === "custom" ? "30m" : "5m";
      let start = "-24h";
      let stop = "now()";
      if (range === "7d") start = "-7d";
      else if (range === "30d") start = "-30d";
      else if (range === "custom") {
        const customStart = url.searchParams.get("start");
        const customStop = url.searchParams.get("stop");
        if (!customStart || !customStop || !Number.isFinite(Date.parse(customStart)) || !Number.isFinite(Date.parse(customStop))) {
          return json(res, 400, { error: "Invalid custom range" });
        }
        start = `time(v: "${new Date(customStart).toISOString()}")`;
        stop = `time(v: "${new Date(customStop).toISOString()}")`;
      }
      const data = await kelloggElectricalHistory(start, stop, aggregate);
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ points: data }));
    } catch (error) {
      res.writeHead(500, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }
  const match = url.pathname.match(/^\/api\/panels\/([^/]+)$/);
  if (match) {
    if (!requireSession(req, res)) return;
    try {
      const data = await panelData(match[1], {
        start: url.searchParams.get("start"),
        stop: url.searchParams.get("stop"),
        window: url.searchParams.get("window")
      });
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify(data));
    } catch (error) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }
  staticFile(req, res);
}).listen(PORT, "0.0.0.0", () => console.log(`EMS dashboard listening on ${PORT}`));
