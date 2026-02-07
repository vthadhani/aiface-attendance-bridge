const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

function ensureDir(filePath) {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
}

function initDb(sqlitePath) {
  ensureDir(sqlitePath);

  const db = new Database(sqlitePath);
  db.pragma("journal_mode = WAL");

  db.exec(`
    CREATE TABLE IF NOT EXISTS punches (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_sn TEXT,
      enrollid INTEGER,
      punch_time TEXT,
      inout INTEGER,
      mode INTEGER,
      event INTEGER,
      verifymode INTEGER,
      temp REAL,
      image_base64 TEXT,
      raw_json TEXT,
      received_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_punches_time ON punches(punch_time);
    CREATE INDEX IF NOT EXISTS idx_punches_enrollid ON punches(enrollid);
    CREATE INDEX IF NOT EXISTS idx_punches_device_sn ON punches(device_sn);
  `);

  const insertStmt = db.prepare(`
    INSERT INTO punches (
      device_sn, enrollid, punch_time, inout, mode, event, verifymode, temp, image_base64, raw_json, received_at
    ) VALUES (
      @device_sn, @enrollid, @punch_time, @inout, @mode, @event, @verifymode, @temp, @image_base64, @raw_json, @received_at
    )
  `);

  return {
    insertPunch(p) {
      insertStmt.run(p);
    },
    listLatest(limit) {
      const stmt = db.prepare(`
        SELECT *
        FROM punches
        ORDER BY datetime(punch_time) DESC, id DESC
        LIMIT ?
      `);
      return stmt.all(limit);
    },
    listLogs({ since, limit, offset }) {
      if (since) {
        const stmt = db.prepare(`
          SELECT *
          FROM punches
          WHERE datetime(punch_time) >= datetime(?)
          ORDER BY datetime(punch_time) DESC, id DESC
          LIMIT ? OFFSET ?
        `);
        return stmt.all(since, limit, offset);
      }

      const stmt = db.prepare(`
        SELECT *
        FROM punches
        ORDER BY datetime(punch_time) DESC, id DESC
        LIMIT ? OFFSET ?
      `);
      return stmt.all(limit, offset);
    },
    listByEmployee({ enrollid, limit, offset }) {
      const stmt = db.prepare(`
        SELECT *
        FROM punches
        WHERE enrollid = ?
        ORDER BY datetime(punch_time) DESC, id DESC
        LIMIT ? OFFSET ?
      `);
      return stmt.all(enrollid, limit, offset);
    }
  };
}

module.exports = { initDb };
