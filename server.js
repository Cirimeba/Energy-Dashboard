const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT || 8080);
const INFLUX_URL = (process.env.INFLUX_URL || "").replace(/\/$/, "");
const INFLUX_ORG = process.env.INFLUX_ORG || "botlabs";
const INFLUX_BUCKET = process.env.INFLUX_BUCKET || "energia";
const INFLUX_TOKEN = process.env.INFLUX_TOKEN || "";
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
    '|> range(start: -30d)',
    '|> filter(fn: (r) => r["_field"] == "SQLDB_GN_nm3")',
    '|> last()'
  ].join("\n");

  const demandRows = await executeQuery(demandQuery);
  const generationRows = await executeQuery(generationQuery);
  const gasRows = await executeQuery(gasQuery);
  const demand = demandRows[demandRows.length - 1];
  const generation = generationRows[generationRows.length - 1];
  const gas = gasRows[gasRows.length - 1];

  const value = demand ? Number(demand._value) : NaN;
  const cogenerationValue = generation ? Number(generation._value) : NaN;
  const gasValue = gas ? Number(gas._value) : NaN;

  return {
    value: Number.isFinite(value) ? value : null,
    unit: "kW",
    time: demand ? demand._time : null,
    cogenerationValue: Number.isFinite(cogenerationValue) ? cogenerationValue : null,
    cogenerationUnit: "kW",
    cogenerationTime: generation ? generation._time : null,
    gasValue: Number.isFinite(gasValue) ? gasValue : null,
    gasUnit: "Nm³",
    gasTime: gas ? gas._time : null
  };
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
    res.writeHead(200, { "Content-Type": TYPES[path.extname(filename)] || "application/octet-stream", "Cache-Control": "no-cache" });
    res.end(data);
  });
}

http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname === "/api/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", influxConfigured: Boolean(INFLUX_URL && INFLUX_TOKEN) }));
    return;
  }
  if (url.pathname === "/api/actual") {
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
  const match = url.pathname.match(/^\/api\/panels\/([^/]+)$/);
  if (match) {
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
