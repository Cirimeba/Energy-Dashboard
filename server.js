const http = require("http");
const fs = require("fs");
const path = require("path");

const port = Number(process.env.PORT || 8080);
const influxUrl = (process.env.INFLUX_URL || "").replace(/\/$/, "");
const organization = process.env.INFLUX_ORG || "";
const bucket = process.env.INFLUX_BUCKET || "";
const token = process.env.INFLUX_TOKEN || "";
const publicDir = __dirname;

const contentTypes = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml"
};

function parseCsvLine(line) {
  const result = [];
  let value = "";
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      if (quoted && line[i + 1] === '"') {
        value += '"';
        i += 1;
      } else {
        quoted = !quoted;
      }
    } else if (char === "," && !quoted) {
      result.push(value);
      value = "";
    } else {
      value += char;
    }
  }
  result.push(value);
  return result;
}

function parseAnnotatedCsv(text) {
  const records = [];
  const lines = text.split(/\r?\n/).filter(Boolean);
  let header = null;
  for (const line of lines) {
    if (line.startsWith("#")) continue;
    const row = parseCsvLine(line);
    if (!header) {
      header = row;
      continue;
    }
    if (row.length !== header.length) continue;
    records.push(Object.fromEntries(header.map((key, index) => [key, row[index]])));
  }
  return records;
}

function latestByField(records) {
  const values = {};
  for (const record of records) {
    const field = record._field;
    const time = record._time;
    if (!field || !time || !Number.isFinite(Number(record._value))) continue;
    if (!values[field] || Date.parse(time) > Date.parse(values[field]._time)) {
      values[field] = { _time: time, value: Number(record._value) };
    }
  }
  return Object.fromEntries(Object.entries(values).map(([field, item]) => [field, item.value]));
}

async function queryInflux(measurement, fields) {
  const query = `from(bucket: "${bucket}")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "${measurement}")
  |> filter(fn: (r) => r._field =~ /${fields.join("|")}/)
  |> last()`;
  const response = await fetch(`${influxUrl}/api/v2/query?org=${encodeURIComponent(organization)}`, {
    method: "POST",
    headers: {
      Authorization: `Token ${token}`,
      "Content-Type": "application/vnd.flux",
      Accept: "application/csv"
    },
    body: query
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`InfluxDB ${response.status}: ${text.slice(0, 300)}`);
  return latestByField(parseAnnotatedCsv(text));
}

async function telemetry() {
  if (!influxUrl || !organization || !bucket || !token) {
    return { configured: false, source: "mock" };
  }

  async function energyHistory(start = "-24h") {
    const fluxStart = ["-24h", "-7d", "-30d"].includes(start) ? start : "-24h";
    const query = `from(bucket: "${bucket}")
    |> range(start: ${fluxStart})
    |> filter(fn: (r) => r._measurement == "colector_energia" or r._measurement == "modbus")
    |> filter(fn: (r) => r._field =~ /^(SQLDB_(Gen_kwhBase|Gen_kwhInter|Gen_kwhPunta|U_kwhBase|U_kwhInter|U_kwhPunta|GN_nm3)|energia_activa_kwh|potencia_activa_total_w)$/)
    |> sort(columns: ["_time"])`;
    const response = await fetch(`${influxUrl}/api/v2/query?org=${encodeURIComponent(organization)}`, {
      method: "POST",
      headers: {
        Authorization: `Token ${token}`,
        "Content-Type": "application/vnd.flux",
        Accept: "application/csv"
      },
      body: query
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`InfluxDB ${response.status}: ${text.slice(0, 300)}`);
    const series = {};
    for (const record of parseAnnotatedCsv(text)) {
      if (!record._time || !record._field || !Number.isFinite(Number(record._value))) continue;
      if (!series[record._field]) series[record._field] = [];
      series[record._field].push({ time: record._time, value: Number(record._value) });
    }
    return series;
  }
  const collectorFields = [
    "SQLDB_GN_nm3",
    "SQLDB_Gen_kwhBase",
    "SQLDB_Gen_kwhInter",
    "SQLDB_Gen_kwhPunta",
    "SQLDB_U_kwhBase",
    "SQLDB_U_kwhInter",
    "SQLDB_U_kwhPunta",
    "SQLDB_VaporCD_Kgh",
    "SQLDB_VaporCS_Kgh",
    "SQLDB_Vapor_kg"
  ];
  const modbusFields = [
    "potencia_activa_total_w",
    "factor_potencia_total",
    "frecuencia_hz",
    "voltaje_ln_promedio",
    "voltaje_ll_promedio",
    "energia_activa_kwh",
    "corriente_promedio"
  ];
  const [collector, modbus] = await Promise.all([
    queryInflux("colector_energia", collectorFields),
    queryInflux("modbus", modbusFields)
  ]);
  const activeWatts = modbus.potencia_activa_total_w || 0;
  const genKwh = (collector.SQLDB_Gen_kwhBase || 0) + (collector.SQLDB_Gen_kwhInter || 0) + (collector.SQLDB_Gen_kwhPunta || 0);
  const cfeKwh = (collector.SQLDB_U_kwhBase || 0) + (collector.SQLDB_U_kwhInter || 0) + (collector.SQLDB_U_kwhPunta || 0);
  return {
    configured: true,
    source: "influxdb",
    updatedAt: new Date().toISOString(),
    kelloggs: {
      gasNm3: collector.SQLDB_GN_nm3 || 0,
      generationKwh: genKwh,
      cfeKwh,
      steamKgh: collector.SQLDB_VaporCD_Kgh || collector.SQLDB_VaporCS_Kgh || 0,
      steamKg: collector.SQLDB_Vapor_kg || 0
    },
    pringles: {
      activePowerKw: activeWatts / 1000,
      pf: modbus.factor_potencia_total || 0,
      freq: modbus.frecuencia_hz || 0,
      voltageLnV: modbus.voltaje_ln_promedio || 0,
      voltageLlV: modbus.voltaje_ll_promedio || 0,
      activeEnergyKwh: modbus.energia_activa_kwh || 0,
      currentA: modbus.corriente_promedio || 0
    }
  };
}

function serveStatic(req, res) {
  const requested = req.url === "/" ? "/index.html" : req.url.split("?")[0];
  const file = path.resolve(publicDir, "." + requested);
  if (!file.startsWith(publicDir + path.sep)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }
  fs.readFile(file, (error, content) => {
    if (error) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }
    res.writeHead(200, { "Content-Type": contentTypes[path.extname(file)] || "application/octet-stream" });
    res.end(content);
  });
}

const server = http.createServer(async (req, res) => {
  if (req.url === "/api/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", influxConfigured: Boolean(influxUrl && organization && bucket && token) }));
    return;
  }
  if (req.url === "/api/telemetry") {
    try {
      const data = await telemetry();
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify(data));
    } catch (error) {
      console.error(error);
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ configured: true, source: "error", error: error.message }));
    }
    return;
  }
  if (req.url.startsWith("/api/energy-history")) {
    try {
      if (!influxUrl || !organization || !bucket || !token) {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ configured: false, series: {} }));
        return;
      }
      const url = new URL(req.url, "http://localhost");
      const series = await energyHistory(url.searchParams.get("range") || "-24h");
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ configured: true, series }));
    } catch (error) {
      console.error(error);
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ configured: true, error: error.message }));
    }
    return;
  }
  serveStatic(req, res);
});

server.listen(port, "0.0.0.0", () => console.log(`Energy dashboard listening on port ${port}`));
