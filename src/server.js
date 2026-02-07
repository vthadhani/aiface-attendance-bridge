require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const morgan = require("morgan");
const mqtt = require("mqtt");

const { initDb } = require("./db");

const PORT = parseInt(process.env.PORT || "3000", 10);
const MQTT_URL = process.env.MQTT_URL;
const MQTT_SUB_TOPIC = process.env.MQTT_SUB_TOPIC || "aiface/+/sub";
const SQLITE_PATH = process.env.SQLITE_PATH || "/app/data/attendance.sqlite";
const API_TOKEN = process.env.API_TOKEN || "";

if (!MQTT_URL) {
  console.error("ERROR: MQTT_URL is required.");
  process.exit(1);
}
if (!API_TOKEN) {
  console.error("ERROR: API_TOKEN is required.");
  process.exit(1);
}

const db = initDb(SQLITE_PATH);

function authMiddleware(req, res, next) {
  const header = req.headers.authorization || "";
  const expected = `Bearer ${API_TOKEN}`;
  if (header !== expected) return res.status(401).json({ error: "Unauthorized" });
  next();
}

function safeJson(payload) {
  try {
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

function normalizePunch(deviceSn, msg, rec) {
  const punch_time = rec.time || msg.cloudtime || new Date().toISOString();
  return {
    device_sn: deviceSn || msg.sn || null,
    enrollid: typeof rec.enrollid === "number" ? rec.enrollid : parseInt(rec.enrollid || "0", 10),
    punch_time: String(punch_time),
    inout: rec.inout === undefined ? null : Number(rec.inout),
    mode: rec.mode === undefined ? null : Number(rec.mode),
    event: rec.event === undefined ? null : Number(rec.event),
    verifymode: rec.verifymode === undefined ? null : Number(rec.verifymode),
    temp: rec.temp === undefined ? null : Number(rec.temp),
    image_base64: rec.image ? String(rec.image) : null,
    raw_json: JSON.stringify({ msg, rec }),
    received_at: new Date().toISOString()
  };
}

function handleMqttMessage(topic, payloadStr) {
  const msg = safeJson(payloadStr);
  if (!msg || typeof msg !== "object") return;

  if (msg.cmd === "sendlog" && Array.isArray(msg.record)) {
    const deviceSn = msg.sn || null;

    for (const rec of msg.record) {
      const punch = normalizePunch(deviceSn, msg, rec);
      if (!punch.enrollid || punch.enrollid <= 0) continue;
      db.insertPunch(punch);
    }
  }
}

const mqttClient = mqtt.connect(MQTT_URL, {
  clean: true,
  reconnectPeriod: 2000,
  connectTimeout: 20000
});

mqttClient.on("connect", () => {
  console.log("MQTT connected:", MQTT_URL);
  mqttClient.subscribe(MQTT_SUB_TOPIC, { qos: 1 }, (err) => {
    if (err) console.error("MQTT subscribe error:", err.message);
    else console.log("Subscribed:", MQTT_SUB_TOPIC);
  });
});

mqttClient.on("error", (err) => console.error("MQTT error:", err.message));
mqttClient.on("message", (topic, payload) => handleMqttMessage(topic, payload.toString("utf8")));

const app = express();
app.use(helmet());
app.use(express.json({ limit: "2mb" }));
app.use(morgan("combined"));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    mqtt: { url: MQTT_URL, topic: MQTT_SUB_TOPIC, connected: mqttClient.connected },
    db: { sqlite: SQLITE_PATH },
    time: new Date().toISOString()
  });
});

app.use(authMiddleware);

app.get("/logs/latest", (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || "50", 10), 500);
  const rows = db.listLatest(limit);
  res.json({ count: rows.length, limit, rows });
});

app.get("/logs", (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || "200", 10), 1000);
  const offset = Math.max(parseInt(req.query.offset || "0", 10), 0);
  const since = req.query.since ? String(req.query.since) : null;

  const rows = db.listLogs({ since, limit, offset });
  res.json({ count: rows.length, limit, offset, since, rows });
});

app.get("/logs/employee/:enrollid", (req, res) => {
  const enrollid = parseInt(req.params.enrollid, 10);
  if (!enrollid) return res.status(400).json({ error: "Invalid enrollid" });

  const limit = Math.min(parseInt(req.query.limit || "200", 10), 1000);
  const offset = Math.max(parseInt(req.query.offset || "0", 10), 0);

  const rows = db.listByEmployee({ enrollid, limit, offset });
  res.json({ count: rows.length, enrollid, limit, offset, rows });
});

app.listen(PORT, () => console.log(`Bridge API listening on :${PORT}`));
