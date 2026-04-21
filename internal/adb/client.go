package adb

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Device represents a connected Android or iOS device.
type Device struct {
	Serial    string
	ADBSerial string // serial as reported by ADB/peer (original case, e.g. "114582552J101167"); may differ from Serial (normalized lowercase)
	State     string
	Model     string  // ro.product.model (Android) or product name (iOS), populated for ready devices
	ADBHost   string  // per-device ADB host override (hub mode); empty = use global config
	ADBPort   int     // per-device ADB port override (hub mode); 0 = use global config
	Platform  string  // "android" or "ios"; empty is treated as "android"
}

// IsReady returns true if the device is online and ready.
func (d Device) IsReady() bool {
	return d.State == "device"
}

// ListDevices runs `adb devices` and returns the list of detected devices.
func ListDevices() ([]Device, error) {
	out, err := exec.Command("adb", "devices").Output()
	if err != nil {
		return nil, fmt.Errorf("adb devices: %w", err)
	}
	// Skip first line "List of devices attached"
	body := ""
	if idx := strings.Index(string(out), "\n"); idx >= 0 {
		body = string(out)[idx+1:]
	}
	devices := parseDeviceList(body)
	populateModels(devices)
	return devices, nil
}

// parseDeviceList parses the raw device-list payload from the ADB server.
func parseDeviceList(data string) []Device {
	var devices []Device
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		devices = append(devices, Device{Serial: parts[0], State: parts[1]})
	}
	return devices
}

// populateModels fills Model for all ready devices via adb getprop.
func populateModels(devices []Device) {
	for i, d := range devices {
		if !d.IsReady() {
			continue
		}
		if b, err := exec.Command("adb", "-s", d.Serial, "shell", "getprop", "ro.product.model").Output(); err == nil {
			devices[i].Model = strings.TrimSpace(string(b))
		}
	}
}

// TrackDevices connects to the ADB server via TCP and calls onChange whenever
// the device list changes. onReconnect (optional) is called each time the
// connection is re-established so callers can reset any dedup state.
// Reconnects automatically on error. Blocks until ctx is cancelled.
func TrackDevices(ctx context.Context, onChange func([]Device), onReconnect func()) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if onReconnect != nil {
			onReconnect()
		}
		if err := trackOnce(ctx, onChange); err != nil {
			// On a read timeout we reconnect immediately (planned keepalive cycle).
			// On a real error we wait 3s before retrying.
			isTimeout := strings.Contains(err.Error(), "reconnecting")
			if !isTimeout {
				log.Printf("[adb] track-devices: %v — reconnecting in 3s", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
			}
		}
	}
}

func trackOnce(ctx context.Context, onChange func([]Device)) error {
	conn, err := net.DialTimeout("tcp", "localhost:5037", 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Close connection when ctx is done.
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	const cmd = "host:track-devices"
	if _, err := fmt.Fprintf(conn, "%04x%s", len(cmd), cmd); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	status := make([]byte, 4)
	if _, err := io.ReadFull(conn, status); err != nil {
		return fmt.Errorf("read status: %w", err)
	}
	if string(status) != "OKAY" {
		return fmt.Errorf("expected OKAY, got %q", status)
	}

	log.Printf("[adb] track-devices: connected to ADB server")
	const readTimeout = 60 * time.Second
	for {
		// Each update is prefixed with a 4-hex-char length.
		// Set a deadline so we reconnect if ADB goes silent (e.g. daemon restart,
		// stale TCP connection) rather than blocking forever.
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				return fmt.Errorf("read timeout after %s, reconnecting", readTimeout)
			}
			return fmt.Errorf("read length: %w", err)
		}
		conn.SetReadDeadline(time.Time{}) // clear deadline for data read
		n, err := strconv.ParseInt(string(lenBuf), 16, 32)
		if err != nil {
			return fmt.Errorf("parse length %q: %w", lenBuf, err)
		}
		data := make([]byte, n)
		if n > 0 {
			if _, err := io.ReadFull(conn, data); err != nil {
				return fmt.Errorf("read data: %w", err)
			}
		}
		devices := parseDeviceList(string(data))
		populateModels(devices)
		onChange(devices)
	}
}

// adbArgs builds the argument list for an adb command, prepending
// -H host -P port when endpoint is non-empty (hub mode).
// endpoint format: "host:port" or "" for local ADB.
func adbArgs(endpoint, serial string, args ...string) []string {
	var base []string
	if endpoint != "" {
		if i := strings.LastIndex(endpoint, ":"); i > 0 {
			base = append(base, "-H", endpoint[:i], "-P", endpoint[i+1:])
		}
	}
	base = append(base, "-s", serial)
	return append(base, args...)
}

// Reboot sends `adb reboot` to the device.
// endpoint is "host:port" for hub mode or "" for local ADB.
func Reboot(serial, endpoint string) error {
	args := adbArgs(endpoint, serial, "reboot")
	if out, err := exec.Command("adb", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("adb reboot: %w\n%s", err, out)
	}
	return nil
}

// isOnline returns true if the device reports state "device".
func isOnline(serial, endpoint string) bool {
	args := adbArgs(endpoint, serial, "get-state")
	out, err := exec.Command("adb", args...).Output()
	return err == nil && strings.TrimSpace(string(out)) == "device"
}

// isBootCompleted returns true when Android has finished booting
// (sys.boot_completed=1). ADB becomes reachable well before the system
// finishes starting, so checking only get-state is not enough.
func isBootCompleted(serial, endpoint string) bool {
	args := adbArgs(endpoint, serial, "shell", "getprop", "sys.boot_completed")
	out, err := exec.Command("adb", args...).Output()
	return err == nil && strings.TrimSpace(string(out)) == "1"
}

// WaitForReady blocks until the device is in "device" state AND has fully
// booted (sys.boot_completed=1), or until timeout expires.
// It first waits (up to 30 s) for the device to go offline so we don't return
// prematurely if it hasn't actually started rebooting yet.
// Returns the total elapsed time from the moment it is called.
// endpoint is "host:port" for hub mode or "" for local ADB.
func WaitForReady(serial string, timeout time.Duration, endpoint string) (time.Duration, error) {
	start := time.Now()

	// Phase 1 – wait for the device to go offline (max 30 s).
	offlineDeadline := start.Add(30 * time.Second)
	for time.Now().Before(offlineDeadline) {
		if !isOnline(serial, endpoint) {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Phase 2 – wait for the device to come back AND finish booting.
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		time.Sleep(3 * time.Second)
		if isOnline(serial, endpoint) && isBootCompleted(serial, endpoint) {
			return time.Since(start), nil
		}
	}
	return time.Since(start), fmt.Errorf("device %s not ready after %v", serial, timeout)
}

// GrantAppiumPermissions pre-grants SYSTEM_ALERT_WINDOW and POST_NOTIFICATIONS
// to Appium helper packages so Android does not show permission dialogs during
// test execution. endpoint is "host:port" for hub mode or "" for local ADB.
func GrantAppiumPermissions(serial, endpoint string) {
	pkgs := []string{
		"io.appium.settings",
		"io.appium.uiautomator2.server",
		"io.appium.uiautomator2.server.test",
		"io.appium.android.apis",
	}
	granted := 0
	for _, pkg := range pkgs {
		args := adbArgs(endpoint, serial, "shell", "appops", "set", pkg, "SYSTEM_ALERT_WINDOW", "allow")
		if err := exec.Command("adb", args...).Run(); err == nil {
			granted++
		}
	}
	for _, pkg := range []string{"io.appium.android.apis"} {
		args := adbArgs(endpoint, serial, "shell", "pm", "grant", pkg, "android.permission.POST_NOTIFICATIONS")
		_ = exec.Command("adb", args...).Run()
	}
	if granted > 0 {
		log.Printf("[appium] granted SYSTEM_ALERT_WINDOW to %d package(s) on %s", granted, serial)
	}
	// Set default USB function to MTP so Android doesn't show the
	// "USB-подключение" mode-selection dialog after reboot.
	args := adbArgs(endpoint, serial, "shell", "svc", "usb", "setFunctions", "mtp")
	_ = exec.Command("adb", args...).Run()
}

// TakeScreenshot captures a PNG screenshot from the device.
// endpoint is "host:port" for hub mode or "" for local ADB.
func TakeScreenshot(serial, endpoint string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	args := adbArgs(endpoint, serial, "exec-out", "screencap", "-p")
	return exec.CommandContext(ctx, "adb", args...).Output()
}

// androidVendors maps USB vendor IDs (lowercase hex) to OEM names.
var androidVendors = map[string]string{
	"0489": "Foxconn",
	"04c5": "Fujitsu",
	"04dd": "Sharp",
	"04e8": "Samsung",
	"0502": "Acer",
	"05c6": "Qualcomm",
	"0b05": "Asus",
	"0bb4": "HTC",
	"0e8d": "Tecno/Infinix",
	"0fce": "Sony Ericsson",
	"1004": "LG",
	"12d1": "Huawei",
	"17ef": "Lenovo",
	"18d1": "Google",
	"19d2": "ZTE",
	"1782": "Unisoc",
	"1bbb": "Alcatel",
	"1d4d": "Pegatron",
	"1ebf": "Huawei",
	"1f3a": "Xiaomi",
	"22b8": "Motorola",
	"22d9": "Realme",
	"2717": "Xiaomi/Poco/Redmi",
	"2a45": "Meizu",
	"2a70": "OPPO/Realme/OnePlus",
	"2d95": "vivo",
	"2e04": "Nokia",
	"2ee5": "Fairphone",
	"339b": "Honor",
}

// USBAndroidDevice describes an Android device detected via USB sysfs,
// regardless of whether ADB can see it.
type USBAndroidDevice struct {
	Path    string `json:"path"`
	VID     string `json:"vid"`
	PID     string `json:"pid"`
	Serial  string `json:"serial"`   // USB serial descriptor (may be empty)
	Product string `json:"product"`  // USB product string
	Vendor  string `json:"vendor"`   // human-readable OEM name
	InADB   bool   `json:"in_adb"`  // true if visible in `adb devices`
}

// USBAndroidDevices enumerates all USB devices with known Android vendor IDs
// by reading /sys/bus/usb/devices. Only device nodes (no interface suffixes)
// are returned.
func USBAndroidDevices() []USBAndroidDevice {
	entries, err := os.ReadDir("/sys/bus/usb/devices")
	if err != nil {
		return nil
	}
	var out []USBAndroidDevice
	for _, entry := range entries {
		name := entry.Name()
		// Skip interface nodes (e.g. "2-1.3:1.0") — device nodes have no colon.
		if strings.ContainsRune(name, ':') {
			continue
		}
		dir := "/sys/bus/usb/devices/" + name
		vidB, err := os.ReadFile(dir + "/idVendor")
		if err != nil {
			continue
		}
		vid := strings.TrimSpace(string(vidB))
		oem, ok := androidVendors[vid]
		if !ok {
			continue
		}
		dev := USBAndroidDevice{Path: name, VID: vid, Vendor: oem}
		if b, err := os.ReadFile(dir + "/idProduct"); err == nil {
			dev.PID = strings.TrimSpace(string(b))
		}
		if b, err := os.ReadFile(dir + "/serial"); err == nil {
			dev.Serial = strings.TrimSpace(string(b))
		}
		if b, err := os.ReadFile(dir + "/product"); err == nil {
			dev.Product = strings.TrimSpace(string(b))
		}
		out = append(out, dev)
	}
	return out
}

// USBInfo returns the sysfs USB path, vendor ID and product ID for the device
// with the given ADB serial by scanning /sys/bus/usb/devices/. Returns empty
// strings if the device is not found (e.g. already disconnected).
func USBInfo(serial string) (path, vid, pid string) {
	entries, err := os.ReadDir("/sys/bus/usb/devices")
	if err != nil {
		return
	}
	for _, entry := range entries {
		dir := "/sys/bus/usb/devices/" + entry.Name()
		b, err := os.ReadFile(dir + "/serial")
		if err != nil || strings.TrimSpace(string(b)) != serial {
			continue
		}
		path = entry.Name()
		if b, err := os.ReadFile(dir + "/idVendor"); err == nil {
			vid = strings.TrimSpace(string(b))
		}
		if b, err := os.ReadFile(dir + "/idProduct"); err == nil {
			pid = strings.TrimSpace(string(b))
		}
		return
	}
	return
}

// BatteryLevel returns the current battery charge level (0–100) for the device.
// Returns -1 and a non-nil error if the level cannot be determined.
// endpoint is "host:port" for hub mode or "" for local ADB.
func BatteryLevel(serial, endpoint string) (int, error) {
	args := adbArgs(endpoint, serial, "shell", "dumpsys", "battery")
	out, err := exec.Command("adb", args...).Output()
	if err != nil {
		return -1, fmt.Errorf("adb dumpsys battery: %w", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "level:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "level:"))
			n, err := strconv.Atoi(val)
			if err != nil {
				return -1, fmt.Errorf("parse battery level %q: %w", val, err)
			}
			return n, nil
		}
	}
	return -1, fmt.Errorf("battery level not found in dumpsys output")
}

// EnsureServerListensOnAllInterfaces restarts the ADB server with the -a flag
// so containers can connect to it over the Docker bridge network.
func EnsureServerListensOnAllInterfaces() error {
	log.Println("Restarting ADB server to listen on all interfaces (-a)...")

	// Kill existing server
	if out, err := exec.Command("adb", "kill-server").CombinedOutput(); err != nil {
		return fmt.Errorf("adb kill-server: %w\n%s", err, out)
	}

	// Start new server listening on all interfaces.
	// Use "start-server" (not "nodaemon") so ADB spawns a proper daemon
	// with the user's environment and correct ~/.android/adbkey path —
	// otherwise devices may appear as "unauthorized".
	if out, err := exec.Command("adb", "-a", "start-server").CombinedOutput(); err != nil {
		return fmt.Errorf("adb start-server: %w\n%s", err, out)
	}

	log.Println("ADB server started (listening on all interfaces)")
	return nil
}
