package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

var msk = time.FixedZone("MSK", 3*3600)

const schema = `
CREATE TABLE IF NOT EXISTS runs (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	serial        TEXT    NOT NULL,
	model         TEXT    NOT NULL,
	finished_at   TEXT    NOT NULL,
	passing       INTEGER NOT NULL DEFAULT 0,
	failing       INTEGER NOT NULL DEFAULT 0,
	pending       INTEGER NOT NULL DEFAULT 0,
	found         INTEGER NOT NULL DEFAULT 0,
	boot_ok       INTEGER NOT NULL DEFAULT 0,
	boot_seconds  REAL    NOT NULL DEFAULT 0,
	total_seconds REAL    NOT NULL DEFAULT 0,
	test_seconds  REAL    NOT NULL DEFAULT 0,
	has_logs       INTEGER NOT NULL DEFAULT 0,
	has_screenshot INTEGER NOT NULL DEFAULT 0,
	battery_pct    INTEGER NOT NULL DEFAULT -1,
	battery_temp   REAL    NOT NULL DEFAULT 0,
	usb_path        TEXT    NOT NULL DEFAULT '',
	session_ms      INTEGER NOT NULL DEFAULT 0,
	apk_ms          INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_runs_serial   ON runs(serial);
CREATE INDEX IF NOT EXISTS idx_runs_finished ON runs(finished_at);
CREATE TABLE IF NOT EXISTS device_events (
	id       INTEGER PRIMARY KEY AUTOINCREMENT,
	serial   TEXT NOT NULL,
	model    TEXT NOT NULL DEFAULT '',
	event    TEXT NOT NULL,           -- 'connected' | 'disconnected'
	ts       TEXT NOT NULL,           -- RFC3339 UTC
	usb_path TEXT NOT NULL DEFAULT '', -- sysfs path, e.g. "1-3.2"
	vid      TEXT NOT NULL DEFAULT '', -- USB vendor ID hex, e.g. "04e8"
	pid      TEXT NOT NULL DEFAULT ''  -- USB product ID hex, e.g. "6860"
);
CREATE INDEX IF NOT EXISTS idx_events_serial ON device_events(serial);
CREATE INDEX IF NOT EXISTS idx_events_ts     ON device_events(ts);
CREATE TABLE IF NOT EXISTS usb_events (
	id      INTEGER PRIMARY KEY AUTOINCREMENT,
	ts      TEXT NOT NULL,           -- RFC3339 UTC
	event   TEXT NOT NULL,           -- 'appeared' | 'disappeared' | 'mode_change'
	path    TEXT NOT NULL DEFAULT '', -- sysfs USB path, e.g. "1-2.1"
	vid     TEXT NOT NULL DEFAULT '',
	pid     TEXT NOT NULL DEFAULT '',
	serial  TEXT NOT NULL DEFAULT '', -- USB serial descriptor
	product TEXT NOT NULL DEFAULT '', -- USB product string
	vendor  TEXT NOT NULL DEFAULT '', -- human-readable OEM name
	in_adb  INTEGER NOT NULL DEFAULT 0,
	detail  TEXT NOT NULL DEFAULT ''  -- extra info, e.g. "22d9:2769 → 18d1:4ee8"
);
CREATE INDEX IF NOT EXISTS idx_usb_ts     ON usb_events(ts);
CREATE INDEX IF NOT EXISTS idx_usb_serial ON usb_events(serial);
CREATE TABLE IF NOT EXISTS device_config (
	serial   TEXT PRIMARY KEY,
	test_vid TEXT NOT NULL DEFAULT '',
	test_pid TEXT NOT NULL DEFAULT ''
);
`

// migrations adds columns/tables to existing databases that predate the current schema.
var migrations = []string{
	`ALTER TABLE runs ADD COLUMN total_seconds   REAL    NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN test_seconds    REAL    NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN has_logs        INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN has_screenshot  INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN battery_pct     INTEGER NOT NULL DEFAULT -1`,
	`ALTER TABLE runs ADD COLUMN usb_path        TEXT    NOT NULL DEFAULT ''`,
	`ALTER TABLE runs ADD COLUMN session_ms      INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN apk_ms          INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN battery_temp   REAL    NOT NULL DEFAULT 0`,
	// device_events table (new in v1.8.8)
	`CREATE TABLE IF NOT EXISTS device_events (
		id       INTEGER PRIMARY KEY AUTOINCREMENT,
		serial   TEXT NOT NULL,
		model    TEXT NOT NULL DEFAULT '',
		event    TEXT NOT NULL,
		ts       TEXT NOT NULL,
		usb_path TEXT NOT NULL DEFAULT '',
		vid      TEXT NOT NULL DEFAULT '',
		pid      TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE INDEX IF NOT EXISTS idx_events_serial ON device_events(serial)`,
	`CREATE INDEX IF NOT EXISTS idx_events_ts     ON device_events(ts)`,
	// usb_events table (new in v1.8.18)
	`CREATE TABLE IF NOT EXISTS usb_events (
		id      INTEGER PRIMARY KEY AUTOINCREMENT,
		ts      TEXT NOT NULL,
		event   TEXT NOT NULL,
		path    TEXT NOT NULL DEFAULT '',
		vid     TEXT NOT NULL DEFAULT '',
		pid     TEXT NOT NULL DEFAULT '',
		serial  TEXT NOT NULL DEFAULT '',
		product TEXT NOT NULL DEFAULT '',
		vendor  TEXT NOT NULL DEFAULT '',
		in_adb  INTEGER NOT NULL DEFAULT 0
	)`,
	`CREATE INDEX IF NOT EXISTS idx_usb_ts     ON usb_events(ts)`,
	`CREATE INDEX IF NOT EXISTS idx_usb_serial ON usb_events(serial)`,
	`ALTER TABLE usb_events ADD COLUMN detail TEXT NOT NULL DEFAULT ''`,
	// device_config table (new in v1.8.59)
	`CREATE TABLE IF NOT EXISTS device_config (
		serial   TEXT PRIMARY KEY,
		test_vid TEXT NOT NULL DEFAULT '',
		test_pid TEXT NOT NULL DEFAULT ''
	)`,
}

// Run holds the result of one test cycle for one device.
type Run struct {
	ID           int64     `json:"id"`
	Serial       string    `json:"serial"`
	Model        string    `json:"model"`
	FinishedAt   time.Time `json:"finished_at"`
	Passing      int       `json:"passing"`
	Failing      int       `json:"failing"`
	Pending      int       `json:"pending"`
	Found        bool      `json:"found"`
	BootOK       bool      `json:"boot_ok"`
	BootSeconds  float64   `json:"boot_seconds"`
	TotalSeconds float64   `json:"total_seconds"`
	TestSeconds  float64   `json:"test_seconds"`
	HasLogs       bool   `json:"has_logs"`
	HasScreenshot bool   `json:"has_screenshot"`
	BatteryPct    int     `json:"battery_pct"`  // battery level at test start; -1 = unknown
	BatteryTemp   float64 `json:"battery_temp"` // temperature °C at test start; 0 = unknown
	UsbPath       string  `json:"usb_path"`     // sysfs USB path at time of run, e.g. "1-3.2"
	SessionMs     int    `json:"session_ms"`  // Appium POST /session duration in ms
	ApkMs         int    `json:"apk_ms"`      // APK installation duration in ms
}

// Verdict returns "PASS", "FAIL", or "N/A".
func (r Run) Verdict() string {
	if !r.Found {
		return "N/A"
	}
	if r.Failing > 0 {
		return "FAIL"
	}
	return "PASS"
}

// DeviceLabel returns "serial (model)" or just "serial".
func (r Run) DeviceLabel() string {
	if r.Model != "" {
		return fmt.Sprintf("%s (%s)", r.Serial, r.Model)
	}
	return r.Serial
}

// Store wraps the SQLite database.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite supports only one writer at a time
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: schema: %w", err)
	}
	// Apply migrations (ignore errors — column may already exist).
	for _, m := range migrations {
		db.Exec(m) //nolint:errcheck
	}
	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Insert saves a test run to the database and returns the new row ID.
func (s *Store) Insert(r Run) (int64, error) {
	res, err := s.db.Exec(`
		INSERT INTO runs
		  (serial, model, finished_at, passing, failing, pending, found, boot_ok,
		   boot_seconds, total_seconds, test_seconds, battery_pct, battery_temp, usb_path, session_ms, apk_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.Serial,
		r.Model,
		r.FinishedAt.UTC().Format(time.RFC3339),
		r.Passing,
		r.Failing,
		r.Pending,
		boolToInt(r.Found),
		boolToInt(r.BootOK),
		r.BootSeconds,
		r.TotalSeconds,
		r.TestSeconds,
		r.BatteryPct,
		r.BatteryTemp,
		r.UsbPath,
		r.SessionMs,
		r.ApkMs,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// SetHasLogs marks a run as having saved log files.
// UpdateBoot sets boot_ok and boot_seconds on an existing run row.
func (s *Store) UpdateBoot(id int64, bootSeconds float64, bootOK bool) error {
	_, err := s.db.Exec(
		`UPDATE runs SET boot_ok=?, boot_seconds=? WHERE id=?`,
		boolToInt(bootOK), bootSeconds, id,
	)
	return err
}

func (s *Store) SetHasLogs(id int64) error {
	_, err := s.db.Exec(`UPDATE runs SET has_logs=1 WHERE id=?`, id)
	return err
}

// SetHasScreenshot marks a run as having a saved screenshot.
func (s *Store) SetHasScreenshot(id int64) error {
	_, err := s.db.Exec(`UPDATE runs SET has_screenshot=1 WHERE id=?`, id)
	return err
}

// List returns the most recent `limit` runs (newest first).
// If serial is non-empty, only runs for that device are returned.
// from/to are optional time bounds on finished_at (zero value = no bound).
func (s *Store) List(serial string, limit int, from, to time.Time) ([]Run, error) {
	query := `
		SELECT id, serial, model, finished_at, passing, failing, pending, found,
		       boot_ok, boot_seconds, total_seconds, test_seconds, has_logs, has_screenshot, battery_pct, battery_temp, usb_path, session_ms, apk_ms
		FROM runs WHERE 1=1`
	var args []any
	if serial != "" {
		query += ` AND serial = ?`
		args = append(args, serial)
	}
	if !from.IsZero() {
		query += ` AND finished_at >= ?`
		args = append(args, from.UTC().Format(time.RFC3339))
	}
	if !to.IsZero() {
		query += ` AND finished_at <= ?`
		args = append(args, to.UTC().Format(time.RFC3339))
	}
	query += ` ORDER BY finished_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRuns(rows)
}

// DeviceStats holds aggregated statistics for one device.
type DeviceStats struct {
	Serial     string  `json:"serial"`
	Model      string  `json:"model"`
	TotalRuns  int     `json:"total_runs"`  // total number of completed test cycles
	FailedRuns int     `json:"failed_runs"` // cycles where at least one test failed
	TotalTests int     `json:"total_tests"` // sum of passing + failing across all runs
	TotalFail  int     `json:"total_fail"`  // sum of failing tests across all runs
	AvgBoot    float64 `json:"avg_boot"`    // average reboot time in seconds (successful reboots only)
	MinBoot    float64 `json:"min_boot"`
	MaxBoot    float64 `json:"max_boot"`
	AvgTest    float64 `json:"avg_test"`   // average wdio test execution time (found=1 only)
	MinTest    float64 `json:"min_test"`
	MaxTest    float64 `json:"max_test"`
	AvgSetup   float64 `json:"avg_setup"`  // average setup/APK-install time (total - test, found=1 only)
	MinSetup   float64 `json:"min_setup"`
	MaxSetup   float64 `json:"max_setup"`
	UsbPath     string  `json:"usb_path"`     // last known USB path from device_events
	LastBattery int     `json:"last_battery"`  // battery_pct from most recent run; -1 = unknown
	LastBatteryTemp float64 `json:"last_battery_temp"` // temperature °C from most recent run
	AvgSession  float64 `json:"avg_session"`
	MinSession  float64 `json:"min_session"`
	MaxSession  float64 `json:"max_session"`
	AvgApk      float64 `json:"avg_apk"`
	MinApk      float64 `json:"min_apk"`
	MaxApk      float64 `json:"max_apk"`
}

// PassRate returns the percentage of passing tests (0–100).
func (s DeviceStats) PassRate() float64 {
	if s.TotalTests == 0 {
		return 0
	}
	return float64(s.TotalTests-s.TotalFail) / float64(s.TotalTests) * 100
}

// DeviceLabel returns "serial (model)" or just "serial".
func (s DeviceStats) DeviceLabel() string {
	if s.Model != "" {
		return fmt.Sprintf("%s (%s)", s.Serial, s.Model)
	}
	return s.Serial
}

// Stats returns per-device aggregated statistics, ordered by most recent run.
// from/to are optional time bounds on finished_at (zero value = no bound).
func (s *Store) Stats(from, to time.Time) ([]DeviceStats, error) {
	query := `
		SELECT
			serial,
			MAX(model) AS model,
			COUNT(*)   AS total_runs,
			SUM(CASE WHEN found=1 AND failing>0 THEN 1 ELSE 0 END) AS failed_runs,
			SUM(passing + failing) AS total_tests,
			SUM(failing)           AS total_fail,
			AVG(CASE WHEN boot_ok=1 THEN boot_seconds END) AS avg_boot,
			MIN(CASE WHEN boot_ok=1 THEN boot_seconds END) AS min_boot,
			MAX(CASE WHEN boot_ok=1 THEN boot_seconds END) AS max_boot,
			AVG(CASE WHEN found=1 AND test_seconds>0 THEN test_seconds END)                     AS avg_test,
			MIN(CASE WHEN found=1 AND test_seconds>0 THEN test_seconds END)                     AS min_test,
			MAX(CASE WHEN found=1 AND test_seconds>0 THEN test_seconds END)                     AS max_test,
			AVG(CASE WHEN found=1 AND total_seconds>test_seconds THEN total_seconds-test_seconds END) AS avg_setup,
			MIN(CASE WHEN found=1 AND total_seconds>test_seconds THEN total_seconds-test_seconds END) AS min_setup,
			MAX(CASE WHEN found=1 AND total_seconds>test_seconds THEN total_seconds-test_seconds END) AS max_setup,
			AVG(CASE WHEN session_ms>0 THEN session_ms END) AS avg_session,
			MIN(CASE WHEN session_ms>0 THEN session_ms END) AS min_session,
			MAX(CASE WHEN session_ms>0 THEN session_ms END) AS max_session,
			AVG(CASE WHEN apk_ms>0 THEN apk_ms END)         AS avg_apk,
			MIN(CASE WHEN apk_ms>0 THEN apk_ms END)         AS min_apk,
			MAX(CASE WHEN apk_ms>0 THEN apk_ms END)         AS max_apk
		FROM runs WHERE 1=1`
	var args []any
	if !from.IsZero() {
		query += ` AND finished_at >= ?`
		args = append(args, from.UTC().Format(time.RFC3339))
	}
	if !to.IsZero() {
		query += ` AND finished_at <= ?`
		args = append(args, to.UTC().Format(time.RFC3339))
	}
	query += ` GROUP BY serial ORDER BY COALESCE(NULLIF(model,''), serial) ASC`

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []DeviceStats
	for rows.Next() {
		var st DeviceStats
		var avgBoot, minBoot, maxBoot sql.NullFloat64
		var avgTest, minTest, maxTest sql.NullFloat64
		var avgSetup, minSetup, maxSetup sql.NullFloat64
		var avgSession, minSession, maxSession sql.NullFloat64
		var avgApk, minApk, maxApk sql.NullFloat64
		if err := rows.Scan(
			&st.Serial, &st.Model,
			&st.TotalRuns, &st.FailedRuns,
			&st.TotalTests, &st.TotalFail,
			&avgBoot, &minBoot, &maxBoot,
			&avgTest, &minTest, &maxTest,
			&avgSetup, &minSetup, &maxSetup,
			&avgSession, &minSession, &maxSession,
			&avgApk, &minApk, &maxApk,
		); err != nil {
			return nil, err
		}
		st.AvgBoot = avgBoot.Float64
		st.MinBoot = minBoot.Float64
		st.MaxBoot = maxBoot.Float64
		st.AvgTest = avgTest.Float64
		st.MinTest = minTest.Float64
		st.MaxTest = maxTest.Float64
		st.AvgSetup = avgSetup.Float64
		st.MinSetup = minSetup.Float64
		st.MaxSetup = maxSetup.Float64
		st.AvgSession = avgSession.Float64
		st.MinSession = minSession.Float64
		st.MaxSession = maxSession.Float64
		st.AvgApk = avgApk.Float64
		st.MinApk = minApk.Float64
		st.MaxApk = maxApk.Float64
		stats = append(stats, st)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Fill UsbPath and LastBattery from most recent records per serial.
	for i := range stats {
		var path sql.NullString
		_ = s.db.QueryRow(
			`SELECT usb_path FROM device_events WHERE serial=? ORDER BY ts DESC LIMIT 1`,
			stats[i].Serial,
		).Scan(&path)
		stats[i].UsbPath = path.String

		var batt sql.NullInt64
		var battTemp sql.NullFloat64
		_ = s.db.QueryRow(
			`SELECT battery_pct, battery_temp FROM runs WHERE serial=? ORDER BY finished_at DESC LIMIT 1`,
			stats[i].Serial,
		).Scan(&batt, &battTemp)
		if batt.Valid {
			stats[i].LastBattery = int(batt.Int64)
		} else {
			stats[i].LastBattery = -1
		}
		stats[i].LastBatteryTemp = battTemp.Float64
	}
	return stats, nil
}

// Devices returns all unique serial numbers that have runs, ordered by most recent.
func (s *Store) Devices() ([]string, error) {
	rows, err := s.db.Query(`
		SELECT serial FROM runs GROUP BY serial ORDER BY MAX(finished_at) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var serials []string
	for rows.Next() {
		var serial string
		if err := rows.Scan(&serial); err != nil {
			return nil, err
		}
		serials = append(serials, serial)
	}
	return serials, rows.Err()
}

func scanRuns(rows *sql.Rows) ([]Run, error) {
	var runs []Run
	for rows.Next() {
		var r Run
		var finishedAt string
		var found, bootOK, hasLogs, hasScreenshot int
		if err := rows.Scan(
			&r.ID, &r.Serial, &r.Model, &finishedAt,
			&r.Passing, &r.Failing, &r.Pending,
			&found, &bootOK,
			&r.BootSeconds, &r.TotalSeconds, &r.TestSeconds,
			&hasLogs, &hasScreenshot, &r.BatteryPct, &r.BatteryTemp, &r.UsbPath,
			&r.SessionMs, &r.ApkMs,
		); err != nil {
			return nil, err
		}
		t, _ := time.Parse(time.RFC3339, finishedAt)
		r.FinishedAt = t.In(msk)
		r.Found = found != 0
		r.BootOK = bootOK != 0
		r.HasLogs = hasLogs != 0
		r.HasScreenshot = hasScreenshot != 0
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// DeviceEvent records a single connect or disconnect event for a device.
type DeviceEvent struct {
	ID      int64     `json:"id"`
	Serial  string    `json:"serial"`
	Model   string    `json:"model"`
	Event   string    `json:"event"`    // "connected" | "disconnected"
	TS      time.Time `json:"ts"`
	USBPath string    `json:"usb_path"` // sysfs device path, e.g. "1-3.2"
	VID     string    `json:"vid"`      // USB vendor ID hex, e.g. "04e8"
	PID     string    `json:"pid"`      // USB product ID hex, e.g. "6860"
}

// InsertEvent saves a device connection event to the database.
func (s *Store) InsertEvent(e DeviceEvent) error {
	_, err := s.db.Exec(`
		INSERT INTO device_events (serial, model, event, ts, usb_path, vid, pid)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		e.Serial, e.Model, e.Event,
		e.TS.UTC().Format(time.RFC3339),
		e.USBPath, e.VID, e.PID,
	)
	return err
}

// ListEvents returns the most recent `limit` device events (newest first).
// If serial is non-empty, only events for that device are returned.
func (s *Store) ListEvents(serial string, limit int) ([]DeviceEvent, error) {
	query := `SELECT id, serial, model, event, ts, usb_path, vid, pid
	          FROM device_events WHERE 1=1`
	var args []any
	if serial != "" {
		query += ` AND serial = ?`
		args = append(args, serial)
	}
	query += ` ORDER BY ts DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []DeviceEvent
	for rows.Next() {
		var e DeviceEvent
		var ts string
		if err := rows.Scan(&e.ID, &e.Serial, &e.Model, &e.Event, &ts, &e.USBPath, &e.VID, &e.PID); err != nil {
			return nil, err
		}
		t, _ := time.Parse(time.RFC3339, ts)
		e.TS = t.In(msk)
		events = append(events, e)
	}
	return events, rows.Err()
}

// USBEvent records a single USB appear/disappear/mode_change event for an
// Android device that may or may not be visible to ADB.
type USBEvent struct {
	ID      int64     `json:"id"`
	TS      time.Time `json:"ts"`
	Event   string    `json:"event"`   // "appeared" | "disappeared" | "mode_change"
	Path    string    `json:"path"`
	VID     string    `json:"vid"`
	PID     string    `json:"pid"`
	Serial  string    `json:"serial"`
	Product string    `json:"product"`
	Vendor  string    `json:"vendor"`
	InADB   bool      `json:"in_adb"`
	Detail  string    `json:"detail"`  // e.g. "22d9:2769 → 18d1:4ee8" for mode_change
}

// InsertUSBEvent saves a USB device event to the database.
func (s *Store) InsertUSBEvent(e USBEvent) error {
	_, err := s.db.Exec(`
		INSERT INTO usb_events (ts, event, path, vid, pid, serial, product, vendor, in_adb, detail)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.TS.UTC().Format(time.RFC3339),
		e.Event, e.Path, e.VID, e.PID, e.Serial, e.Product, e.Vendor,
		boolToInt(e.InADB), e.Detail,
	)
	return err
}

// ListUSBEvents returns the most recent `limit` USB events (newest first).
// If serial is non-empty, only events for that serial are returned.
func (s *Store) ListUSBEvents(serial string, limit int) ([]USBEvent, error) {
	query := `SELECT id, ts, event, path, vid, pid, serial, product, vendor, in_adb, detail
	          FROM usb_events WHERE 1=1`
	var args []any
	if serial != "" {
		query += ` AND (serial LIKE ? OR product LIKE ?)`
		args = append(args, "%"+serial+"%", "%"+serial+"%")
	}
	query += ` ORDER BY ts DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []USBEvent
	for rows.Next() {
		var e USBEvent
		var ts string
		var inADB int
		if err := rows.Scan(&e.ID, &ts, &e.Event, &e.Path, &e.VID, &e.PID,
			&e.Serial, &e.Product, &e.Vendor, &inADB, &e.Detail); err != nil {
			return nil, err
		}
		t, _ := time.Parse(time.RFC3339, ts)
		e.TS = t.In(msk)
		e.InADB = inADB != 0
		events = append(events, e)
	}
	return events, rows.Err()
}

// LatestUSBInfoPerDevice returns the most recent usb_path/vid/pid from
// device_events for each serial. Used to pre-populate usbCache on startup.
func (s *Store) LatestUSBInfoPerDevice() ([]DeviceEvent, error) {
	rows, err := s.db.Query(`
		SELECT serial, MAX(ts), usb_path, vid, pid
		FROM device_events
		WHERE event = 'connected'
		GROUP BY serial`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []DeviceEvent
	for rows.Next() {
		var e DeviceEvent
		var ts string
		if err := rows.Scan(&e.Serial, &ts, &e.USBPath, &e.VID, &e.PID); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// DeviceConfig holds the test VID:PID filter for a device.
type DeviceConfig struct {
	Serial  string `json:"serial"`
	TestVID string `json:"test_vid"`
	TestPID string `json:"test_pid"`
}

// SetDeviceConfig inserts or replaces the test VID:PID for a device.
func (s *Store) SetDeviceConfig(cfg DeviceConfig) error {
	_, err := s.db.Exec(
		`INSERT INTO device_config (serial, test_vid, test_pid)
		 VALUES (?, ?, ?)
		 ON CONFLICT(serial) DO UPDATE SET test_vid=excluded.test_vid, test_pid=excluded.test_pid`,
		cfg.Serial, cfg.TestVID, cfg.TestPID,
	)
	return err
}

// DeleteDeviceConfig removes the test VID:PID config for a device.
func (s *Store) DeleteDeviceConfig(serial string) error {
	_, err := s.db.Exec(`DELETE FROM device_config WHERE serial=?`, serial)
	return err
}

// AllDeviceConfigs returns all device configs keyed by serial.
func (s *Store) AllDeviceConfigs() (map[string]DeviceConfig, error) {
	rows, err := s.db.Query(`SELECT serial, test_vid, test_pid FROM device_config`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]DeviceConfig)
	for rows.Next() {
		var cfg DeviceConfig
		if err := rows.Scan(&cfg.Serial, &cfg.TestVID, &cfg.TestPID); err != nil {
			return nil, err
		}
		out[cfg.Serial] = cfg
	}
	return out, rows.Err()
}
