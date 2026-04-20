package k8s

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"hub-test/internal/adb"
	"hub-test/internal/store"
	"hub-test/internal/types"

	hubws "xfarm.hub/hub-ws-client"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// Config holds configuration for the K8s manager.
type Config struct {
	AppiumImage          string
	TestImage            string // Android test image; "" = no test containers
	IOSTestImage         string // iOS test image; "" = skip iOS devices
	IOSAppiumImage       string // Appium image for iOS; falls back to AppiumImage
	IPAServeURL          string // IPA URL passed to iOS test container as IOS_IPA_URL
	IOSBundleID          string // bundle ID passed to iOS test container as IOS_BUNDLE_ID
	HubClientImage       string // "" = no hub-client container
	HubUsbmuxdHost       string // USBMUXD_HOST for hub-client (hub-server TCP proxy host)
	HubUsbmuxdPort       string // USBMUXD_PORT for hub-client (default "27015")
	HubHandshakeSecret   string // HANDSHAKE_SECRET for hub-client AES-GCM encryption
	AdbTunnelMode        string // ADB_TUNNEL_MODE for hub-client: "persistent" or "transient" (default)
	APKServeURL          string // e.g. http://hub-test.peer.svc.cluster.local:8080/apk/ApiDemos-debug.apk
	HubURL               string // hub-server base URL e.g. http://hub-server.peer.svc.cluster.local:8080
	Namespace            string
}

// Manager orchestrates K8s Pods for Appium + test execution.
type Manager struct {
	client      *Client
	config      Config
	store       *store.Store
	NotifyFn    func() // called after each run is saved
	ReconcileFn func() // called when a pod finishes to trigger next cycle
	rebooting    sync.Map // serial → struct{}: device is mid-reboot
	reported     sync.Map // podName → struct{}: already processed
	reportMu     sync.Mutex
	adbEndpoints sync.Map // serial → "host:port"
	devices      sync.Map // serial → adb.Device: currently ready devices
	batteryInfo  sync.Map // serial → [2]float64{pct, temp}: battery at last INFO event
	lowBattery   sync.Map // serial → struct{}: pod creation deferred due to low battery
}

// NewManager creates a new K8s Manager.
func NewManager(cli *Client, cfg Config, st *store.Store) *Manager {
	return &Manager{client: cli, config: cfg, store: st}
}

// SetNotify sets the function called after each run is saved.
func (m *Manager) SetNotify(fn func()) { m.NotifyFn = fn }

// SetReconcile sets the function called when a pod finishes.
func (m *Manager) SetReconcile(fn func()) { m.ReconcileFn = fn }

// SetUSBInfo is a no-op in hub/K8s mode (no USB monitor).
func (m *Manager) SetUSBInfo(_, _, _, _ string) {}

// BuildTestImage is a no-op in K8s mode (K8s pulls images).
func (m *Manager) BuildTestImage(_ context.Context, _, _ string) error { return nil }

// PullImage is a no-op in K8s mode (K8s pulls images from registry).
func (m *Manager) PullImage(_ context.Context, _ string) error { return nil }

// RunningDevices returns the devices that currently have an active test pod.
func (m *Manager) RunningDevices(ctx context.Context) []types.RunningDevice {
	pods, err := m.client.ListPods(ctx, labelManaged+"=true")
	if err != nil {
		return nil
	}
	var out []types.RunningDevice
	for _, pod := range pods {
		if pod.Status.Phase != "Running" && pod.Status.Phase != "Pending" {
			continue
		}
		serial := pod.Metadata.Labels[labelDevice]
		if serial == "" {
			continue
		}
		out = append(out, types.RunningDevice{
			Serial:  serial,
			HasTest: true,
		})
	}
	return out
}

// Reconcile brings K8s pods in sync with the current device list.
func (m *Manager) Reconcile(ctx context.Context, devices []adb.Device) error {
	// Update per-device ADB endpoints.
	for _, d := range devices {
		if d.ADBHost != "" {
			m.adbEndpoints.Store(d.Serial, fmt.Sprintf("%s:%d", d.ADBHost, d.ADBPort))
		} else {
			m.adbEndpoints.Delete(d.Serial)
		}
	}

	// List existing managed pods.
	pods, err := m.client.ListPods(ctx, labelManaged+"=true")
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}
	podBySerial := make(map[string]*Pod, len(pods))
	for i := range pods {
		p := &pods[i]
		serial := p.Metadata.Labels[labelDevice]
		if serial != "" {
			podBySerial[serial] = p
		}
	}

	// Build the set of ready devices.
	readyDevices := make(map[string]adb.Device)
	for _, d := range devices {
		if d.IsReady() {
			readyDevices[d.Serial] = d
		}
	}

	// Remove pods for disconnected devices.
	for serial, pod := range podBySerial {
		if _, connected := readyDevices[serial]; !connected {
			log.Printf("[k8s] removing pod for disconnected device %s", serial)
			_ = m.client.DeletePod(ctx, pod.Metadata.Name)
		}
	}

	// Create or process pods for ready devices.
	for serial, dev := range readyDevices {
		// Skip devices whose platform has no test image configured.
		if dev.Platform == "ios" {
			if m.config.IOSTestImage == "" {
				continue
			}
		} else {
			if m.config.TestImage == "" {
				continue
			}
		}

		pod := podBySerial[serial]

		if pod == nil {
			// No pod yet — create one.
			// If we're waiting for a reboot, the device being back in readyDevices
			// means it has come back online — clear the flag and proceed.
			if _, isRebooting := m.rebooting.Load(serial); isRebooting {
				log.Printf("[k8s] device %s back online after reboot", serial)
				m.rebooting.Delete(serial)
			}
			log.Printf("[k8s] creating test pod for %s (platform=%s)", serial, dev.Platform)
			if err := m.createPod(ctx, dev); err != nil {
				log.Printf("[k8s] create pod for %s: %v", serial, err)
			}
			continue
		}

		switch pod.Status.Phase {
		case "Running":
			// For iOS pods, hub-client is a long-running sidecar: even after the
			// tests container exits the pod stays in "Running" phase. Detect this
			// case by checking whether the "tests" container has already terminated.
			if testsContainerDone(pod) {
				podName := pod.Metadata.Name
				podKey := pod.Metadata.UID
				if podKey == "" {
					podKey = podName
				}
				if _, done := m.reported.LoadOrStore(podKey, struct{}{}); !done {
					log.Printf("[k8s] tests container done in running pod %s, collecting", podName)
					summary := m.processPodResult(ctx, serial, dev.Model, dev.Platform, pod)
					_ = m.client.DeletePod(ctx, podName)
					summary = m.saveTestResult(summary)
					m.rebooting.Store(serial, struct{}{})
					go m.rebootAndReport(summary)
					continue
				}
				// Already reported — just delete.
				log.Printf("[k8s] removing already-reported pod %s", pod.Metadata.Name)
				_ = m.client.DeletePod(ctx, pod.Metadata.Name)
				continue
			}
			log.Printf("[k8s] pod for %s is Running, skipping", serial)

		case "Pending":
			log.Printf("[k8s] pod for %s is Pending, skipping", serial)

		case "Succeeded", "Failed":
			podName := pod.Metadata.Name
			podKey := pod.Metadata.UID
			if podKey == "" {
				podKey = podName // fallback if UID missing
			}
			if _, done := m.reported.LoadOrStore(podKey, struct{}{}); done {
				// Already processed — delete stale pod.
				log.Printf("[k8s] removing already-reported pod %s", podName)
				_ = m.client.DeletePod(ctx, podName)
				continue
			}
			summary := m.processPodResult(ctx, serial, dev.Model, dev.Platform, pod)
			_ = m.client.DeletePod(ctx, podName)
			summary = m.saveTestResult(summary)
			m.rebooting.Store(serial, struct{}{})
			go m.rebootAndReport(summary)

		default:
			// Unknown phase (e.g. "Unknown") — leave it alone.
		}
	}

	return nil
}

// ── DeviceEventHandler implementation (xfarm.hub/hub-ws-client) ──────────────

// OnWelcome logs the initial device list on WebSocket connect.
func (m *Manager) OnWelcome(devices []*hubws.HubDevice) {
	log.Printf("[k8s] welcome: %d device(s)", len(devices))
}

// OnDeviceConnected logs that a device has appeared (peer pod starting, not yet ready).
func (m *Manager) OnDeviceConnected(device *hubws.HubDevice) {
	log.Printf("[k8s] device connected: %s (platform=%s) — waiting for info", device.Serial, device.Platform())
}

// OnDeviceOffline logs that a device went offline (USB unplugged).
func (m *Manager) OnDeviceOffline(serial, platform string) {
	log.Printf("[k8s] device offline: %s (platform=%s)", serial, platform)
	m.adbEndpoints.Delete(serial)
	m.devices.Delete(serial)
	m.lowBattery.Delete(serial)
}

// OnDeviceDeleted removes the test pod when a device is fully removed from hub-server.
func (m *Manager) OnDeviceDeleted(serial, platform string) {
	log.Printf("[k8s] device deleted: %s (platform=%s) — removing test pod", serial, platform)
	m.adbEndpoints.Delete(serial)
	m.devices.Delete(serial)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pods, err := m.client.ListPods(ctx, fmt.Sprintf("%s=true,%s=%s", labelManaged, labelDevice, serial))
	if err != nil {
		log.Printf("[k8s] list pods for deleted device %s: %v", serial, err)
		return
	}
	for i := range pods {
		pod := &pods[i]
		log.Printf("[k8s] deleting pod %s for disconnected device %s", pod.Metadata.Name, serial)
		_ = m.client.DeletePod(ctx, pod.Metadata.Name)
	}
}

// OnDeviceReady is called when the peer pod has sent device info — start tests.
func (m *Manager) OnDeviceReady(hub *hubws.HubDevice) {
	log.Printf("[k8s] device ready: %s (platform=%s, model=%s)", hub.Serial, hub.Platform(), hub.Model())

	// Build adb.Device from hub info.
	peerHost := fmt.Sprintf("peer-%s.%s.svc.cluster.local", hub.Serial, m.config.Namespace)
	dev := adb.Device{
		Serial:   hub.Serial,
		State:    "device",
		Model:    hub.Model(),
		ADBHost:  peerHost,
		ADBPort:  5037,
		Platform: hub.Platform(),
	}

	// Store battery info from device Info payload.
	// Only overwrite existing value if new data is valid — prevents stale -1
	// from overwriting a good reading when the peer reconnects without battery data
	// (e.g. iOS omitempty drops fields when idevicediagnostics returns 0).
	if hub.Info != nil {
		bPct := -1
		var bTemp float64
		// Hub-server embeds battery as nested object: {"battery": {"level": N, "temperature": N}}
		if bat, ok := hub.Info["battery"]; ok {
			if batMap, ok := bat.(map[string]interface{}); ok {
				if v, ok := batMap["level"]; ok {
					if n, ok := v.(float64); ok {
						bPct = int(n)
					}
				}
				if v, ok := batMap["temperature"]; ok {
					if n, ok := v.(float64); ok {
						bTemp = n
					}
				}
			}
		}
		if bPct >= 0 {
			m.batteryInfo.Store(dev.Serial, [2]float64{float64(bPct), bTemp})
		} else if _, exists := m.batteryInfo.Load(dev.Serial); !exists {
			m.batteryInfo.Store(dev.Serial, [2]float64{-1, 0})
		}
	}

	// Store device state and ADB endpoint.
	m.devices.Store(dev.Serial, dev)
	m.adbEndpoints.Store(dev.Serial, fmt.Sprintf("%s:%d", dev.ADBHost, dev.ADBPort))

	// Check platform config.
	if dev.Platform == "ios" && m.config.IOSTestImage == "" {
		log.Printf("[k8s] skipping iOS device %s — no iOS test image configured", dev.Serial)
		return
	}
	if dev.Platform != "ios" && m.config.TestImage == "" {
		log.Printf("[k8s] skipping device %s — no test image configured", dev.Serial)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Don't create a pod if one already exists for this device.
	existing, err := m.client.ListPods(ctx, fmt.Sprintf("%s=true,%s=%s", labelManaged, labelDevice, dev.Serial))
	if err != nil {
		log.Printf("[k8s] list pods for %s: %v", dev.Serial, err)
		return
	}
	if len(existing) > 0 {
		log.Printf("[k8s] pod already exists for device %s — skipping", dev.Serial)
		return
	}

	// Clear rebooting flag if set (device came back after reboot).
	if _, isRebooting := m.rebooting.Load(dev.Serial); isRebooting {
		log.Printf("[k8s] device %s back online after reboot", dev.Serial)
		m.rebooting.Delete(dev.Serial)
	}

	// Defer pod creation if battery is critically low.
	if bPct := m.batteryPercent(dev.Serial); bPct >= 0 && bPct <= batteryMinPct {
		log.Printf("[k8s] device %s battery %d%% ≤ %d%% — deferring test pod until charged", dev.Serial, bPct, batteryMinPct)
		m.lowBattery.Store(dev.Serial, dev)
		return
	}

	log.Printf("[k8s] creating test pod for %s (platform=%s)", dev.Serial, dev.Platform)
	if err := m.createPod(ctx, dev); err != nil {
		log.Printf("[k8s] create pod for %s: %v", dev.Serial, err)
	}
}

const (
	batteryMinPct    = 30 // pause tests at or below this level
	batteryResumePct = 35 // resume once battery reaches this level (hysteresis)
)

// batteryPercent returns the last known battery percentage for the device, or -1 if unknown.
func (m *Manager) batteryPercent(serial string) int {
	if v, ok := m.batteryInfo.Load(serial); ok {
		return int(v.([2]float64)[0])
	}
	return -1
}

// OnBatteryUpdated is called by the hub-ws client when a BATTERY UPDATE arrives.
// If the device was deferred due to low battery and level is now sufficient, create the test pod.
func (m *Manager) OnBatteryUpdated(serial, platform string, level, temp int) {
	m.batteryInfo.Store(serial, [2]float64{float64(level), float64(temp)})

	devVal, isDeferred := m.lowBattery.Load(serial)
	if !isDeferred {
		return
	}

	if level < batteryResumePct {
		log.Printf("[k8s] battery update: %s %d%% — still below resume threshold (%d%%)", serial, level, batteryResumePct)
		return
	}

	dev := devVal.(adb.Device)
	m.lowBattery.Delete(serial)
	log.Printf("[k8s] battery update: %s %d%% ≥ %d%% — creating deferred test pod", serial, level, batteryResumePct)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing, err := m.client.ListPods(ctx, fmt.Sprintf("%s=true,%s=%s", labelManaged, labelDevice, dev.Serial))
	if err != nil {
		log.Printf("[k8s] list pods for %s: %v", dev.Serial, err)
		return
	}
	if len(existing) > 0 {
		log.Printf("[k8s] pod already exists for device %s — skipping", dev.Serial)
		return
	}

	if err := m.createPod(ctx, dev); err != nil {
		log.Printf("[k8s] create pod for %s: %v", dev.Serial, err)
	}
}

// CheckPods checks all managed pods and collects results from completed ones.
// Call periodically to handle pod completion between device events.
func (m *Manager) CheckPods(ctx context.Context) {
	pods, err := m.client.ListPods(ctx, labelManaged+"=true")
	if err != nil {
		log.Printf("[k8s] CheckPods list: %v", err)
		return
	}
	for i := range pods {
		pod := &pods[i]
		serial := pod.Metadata.Labels[labelDevice]
		if serial == "" {
			continue
		}
		var dev adb.Device
		if d, ok := m.devices.Load(serial); ok {
			dev = d.(adb.Device)
		} else {
			dev = adb.Device{Serial: serial, Platform: pod.Metadata.Labels[labelPlatform]}
		}

		switch pod.Status.Phase {
		case "Running":
			if testsContainerDone(pod) {
				podKey := pod.Metadata.UID
				if podKey == "" {
					podKey = pod.Metadata.Name
				}
				if _, done := m.reported.LoadOrStore(podKey, struct{}{}); !done {
					log.Printf("[k8s] tests container done in running pod %s, collecting", pod.Metadata.Name)
					summary := m.processPodResult(ctx, serial, dev.Model, dev.Platform, pod)
					_ = m.client.DeletePod(ctx, pod.Metadata.Name)
					summary = m.saveTestResult(summary)
					m.rebooting.Store(serial, struct{}{})
					go m.rebootAndReport(summary)
					if m.ReconcileFn != nil {
						m.ReconcileFn()
					}
				} else {
					log.Printf("[k8s] removing already-reported pod %s", pod.Metadata.Name)
					_ = m.client.DeletePod(ctx, pod.Metadata.Name)
				}
			}

		case "Succeeded", "Failed":
			podKey := pod.Metadata.UID
			if podKey == "" {
				podKey = pod.Metadata.Name
			}
			if _, done := m.reported.LoadOrStore(podKey, struct{}{}); !done {
				summary := m.processPodResult(ctx, serial, dev.Model, dev.Platform, pod)
				_ = m.client.DeletePod(ctx, pod.Metadata.Name)
				summary = m.saveTestResult(summary)
				m.rebooting.Store(serial, struct{}{})
				go m.rebootAndReport(summary)
				if m.ReconcileFn != nil {
					m.ReconcileFn()
				}
			} else {
				log.Printf("[k8s] removing already-reported pod %s", pod.Metadata.Name)
				_ = m.client.DeletePod(ctx, pod.Metadata.Name)
			}
		}
	}
}

// ── Pod creation ─────────────────────────────────────────────────────────────

const (
	labelManaged  = "hub-test.io/managed"
	labelDevice   = "hub-test.io/device"
	labelPlatform = "hub-test.io/platform"
)

func sanitize(s string) string {
	// Replace characters not allowed in K8s names with hyphens.
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

// testsContainerDone returns true when the "tests" container inside a pod has
// already terminated (regardless of exit code). This is used to collect results
// from pods that stay in "Running" phase because a long-lived sidecar (hub-client)
// is still alive.
func testsContainerDone(pod *Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == "tests" {
			return cs.State.Terminated != nil
		}
	}
	return false
}

// iosUDIDFromSerial converts a hub-server serial (24 lowercase hex chars, no hyphen)
// to the UDID format that usbmuxd reports for modern iOS devices:
// "00008030001454190eeb802e" → "00008030-001454190EEB802E"
// If the serial already contains a hyphen it is returned uppercased as-is.
func iosUDIDFromSerial(serial string) string {
	s := strings.ToUpper(serial)
	if strings.Contains(s, "-") {
		return s
	}
	// 24 hex chars → insert hyphen after position 8
	if len(s) == 24 {
		return s[:8] + "-" + s[8:]
	}
	return s
}

func (m *Manager) createPod(ctx context.Context, dev adb.Device) error {
	if dev.Platform == "ios" {
		return m.createIOSPod(ctx, dev)
	}
	return m.createAndroidPod(ctx, dev)
}

func (m *Manager) createAndroidPod(ctx context.Context, dev adb.Device) error {
	podName := "hub-test-" + sanitize(dev.Serial)
	endpoint := m.adbEndpoint(dev.Serial)

	// Parse ADB host from endpoint.
	adbHost := "localhost"
	if endpoint != "" {
		if i := strings.LastIndex(endpoint, ":"); i > 0 {
			adbHost = endpoint[:i]
		}
	}

	volumes := []Volume{
		{Name: "shared", EmptyDir: &EmptyDir{}},
	}
	var containers []Container

	// hub-client container — proxies localhost:5037 → peer ADB server and
	// localhost:8200 → UiAutomator2 port (via sync channel).
	// Required when HubClientImage is set; connects to hub-server TCP proxy.
	if m.config.HubClientImage != "" {
		hubPort := m.config.HubUsbmuxdPort
		if hubPort == "" {
			hubPort = "27015"
		}
		hubClientEnv := []EnvVar{
			{Name: "USBMUXD_HOST", Value: m.config.HubUsbmuxdHost},
			{Name: "USBMUXD_PORT", Value: hubPort},
			{Name: "DEVICE", Value: dev.Serial},
			{Name: "DEVICE_TYPE", Value: "android"},
			{Name: "TZ", Value: "Europe/Moscow"},
		}
		if m.config.HubHandshakeSecret != "" {
			hubClientEnv = append(hubClientEnv, EnvVar{Name: "HANDSHAKE_SECRET", Value: m.config.HubHandshakeSecret})
		}
		if m.config.AdbTunnelMode != "" {
			hubClientEnv = append(hubClientEnv, EnvVar{Name: "TUNNEL_MODE", Value: m.config.AdbTunnelMode})
		}
		// Wrap the client binary so it exits when /shared/done is created
		// (allows the pod to reach Succeeded state after tests finish).
		hubClientScript := `/client &
HID=$!
while [ ! -f /shared/done ]; do sleep 2; done
kill $HID 2>/dev/null
wait $HID 2>/dev/null
exit 0`
		containers = append(containers, Container{
			Name:         "hub-client",
			Image:        m.config.HubClientImage,
			Command:      []string{"sh", "-c"},
			Args:         []string{hubClientScript},
			Env:          hubClientEnv,
			VolumeMounts: []VolumeMount{{Name: "shared", MountPath: "/shared"}},
		})
	}

	// appium container — waits for /shared/done then exits cleanly.
	// With hub-client present, localhost:5037 is the ADB proxy — no override needed.
	appiumScript := `appium --port 4723 --address 0.0.0.0 --allow-insecure=uiautomator2:adb_shell --log-timestamp --log-no-colors &
APID=$!
while [ ! -f /shared/done ]; do sleep 2; done
kill $APID
wait $APID 2>/dev/null
exit 0`
	appiumEnv := []EnvVar{
		{Name: "ANDROID_SERIAL", Value: dev.Serial},
		{Name: "TZ", Value: "Europe/Moscow"},
	}
	// Without hub-client, point Appium directly at the remote peer ADB server.
	if m.config.HubClientImage == "" {
		appiumEnv = append(appiumEnv,
			EnvVar{Name: "ANDROID_ADB_SERVER_ADDRESS", Value: adbHost},
			EnvVar{Name: "ANDROID_ADB_SERVER_PORT", Value: "5037"},
		)
	}
	containers = append(containers, Container{
		Name:         "appium",
		Image:        m.config.AppiumImage,
		Command:      []string{"sh", "-c"},
		Args:         []string{appiumScript},
		Env:          appiumEnv,
		VolumeMounts: []VolumeMount{{Name: "shared", MountPath: "/shared"}},
	})

	// tests container — wraps the image entrypoint, signals done on exit.
	testsScript := `/entrypoint.sh
STATUS=$?
touch /shared/done
exit $STATUS`
	testsEnv := []EnvVar{
		{Name: "ANDROID_SERIAL", Value: dev.Serial},
		{Name: "APPIUM_HOST", Value: "localhost"},
		{Name: "APPIUM_PORT", Value: "4723"},
		{Name: "TZ", Value: "Europe/Moscow"},
	}
	if m.config.APKServeURL != "" {
		testsEnv = append(testsEnv, EnvVar{Name: "APIDEMOS_APK_URL", Value: m.config.APKServeURL})
	}
	containers = append(containers, Container{
		Name:         "tests",
		Image:        m.config.TestImage,
		Command:      []string{"sh", "-c"},
		Args:         []string{testsScript},
		Env:          testsEnv,
		VolumeMounts: []VolumeMount{{Name: "shared", MountPath: "/shared"}},
	})

	pod := &Pod{
		Metadata: ObjectMeta{
			Name:      podName,
			Namespace: m.client.Namespace,
			Labels: map[string]string{
				labelManaged:  "true",
				labelDevice:   dev.Serial,
				labelPlatform: dev.Platform,
			},
		},
		Spec: PodSpec{
			RestartPolicy: "Never",
			Containers:    containers,
			Volumes:       volumes,
		},
	}

	return m.client.CreatePod(ctx, pod)
}

func (m *Manager) createIOSPod(ctx context.Context, dev adb.Device) error {
	podName := "hub-test-" + sanitize(dev.Serial)

	// usbmuxd reports the UDID in the format "XXXXXXXX-YYYYYYYYYYYYYYYYYYY" (hyphen,
	// uppercase hex) which differs from the hub-server serial (lowercase, no hyphen).
	iosUDID := iosUDIDFromSerial(dev.Serial)

	// hub-client tunnels for iOS:
	//   usbmuxd: /var/run/usbmuxd (Unix socket) → hub-server → peer-<serial>:27015
	//   wda:     localhost:7777 (TCP)            → hub-server → peer-<serial>:8100
	// The usbmuxd socket is shared via emptyDir volume so Appium finds it at the standard path.
	const localUsbmuxSocket = "/var/run/usbmuxd"
	const localWDAPort = "7777"

	volumes := []Volume{
		{Name: "shared", EmptyDir: &EmptyDir{}},
		{Name: "usbmuxd-run", EmptyDir: &EmptyDir{}},
	}
	var containers []Container

	// hub-client container — creates the iOS tunnels to hub-server.
	if m.config.HubClientImage != "" {
		hubEnv := []EnvVar{
			{Name: "DEVICE", Value: dev.Serial},
			{Name: "DEVICE_TYPE", Value: "ios"},
			{Name: "USBMUXD_HOST", Value: m.config.HubUsbmuxdHost},
			{Name: "USBMUXD_PORT", Value: m.config.HubUsbmuxdPort},
			{Name: "USBMUXD_SOCKET", Value: localUsbmuxSocket},
			{Name: "HANDSHAKE_SECRET", Value: m.config.HubHandshakeSecret},
			{Name: "TZ", Value: "Europe/Moscow"},
		}
		if m.config.AdbTunnelMode != "" {
			hubEnv = append(hubEnv, EnvVar{Name: "TUNNEL_MODE", Value: m.config.AdbTunnelMode})
		}
		containers = append(containers, Container{
			Name:         "hub-client",
			Image:        m.config.HubClientImage,
			Env:          hubEnv,
			VolumeMounts: []VolumeMount{{Name: "usbmuxd-run", MountPath: "/var/run"}},
		})
	}

	// Appium for iOS (XCUITest driver).
	iosAppiumImage := m.config.IOSAppiumImage
	if iosAppiumImage == "" {
		iosAppiumImage = m.config.AppiumImage
	}
	appiumScript := `appium --port 4723 --address 0.0.0.0 --log-timestamp --log-no-colors &
APID=$!
while [ ! -f /shared/done ]; do sleep 2; done
kill $APID
wait $APID 2>/dev/null
exit 0`
	appiumEnv := []EnvVar{
		{Name: "IOS_UDID", Value: iosUDID},
		{Name: "TZ", Value: "Europe/Moscow"},
	}
	containers = append(containers, Container{
		Name:    "appium",
		Image:   iosAppiumImage,
		Command: []string{"sh", "-c"},
		Args:    []string{appiumScript},
		Env:     appiumEnv,
		VolumeMounts: []VolumeMount{
			{Name: "shared", MountPath: "/shared"},
			{Name: "usbmuxd-run", MountPath: "/var/run"},
		},
	})

	// Tests container.
	testsScript := `/entrypoint.sh
STATUS=$?
touch /shared/done
exit $STATUS`
	testsEnv := []EnvVar{
		{Name: "IOS_UDID", Value: iosUDID},
		{Name: "APPIUM_HOST", Value: "localhost"},
		{Name: "APPIUM_PORT", Value: "4723"},
		// WDA is tunnelled by hub-client: localhost:7777 → hub-server → peer:8100.
		{Name: "WDA_URL", Value: fmt.Sprintf("http://localhost:%s", localWDAPort)},
		{Name: "DEVICE_PLATFORM", Value: "ios"},
		{Name: "TZ", Value: "Europe/Moscow"},
	}
	if m.config.IPAServeURL != "" {
		testsEnv = append(testsEnv, EnvVar{Name: "IOS_IPA_URL", Value: m.config.IPAServeURL})
	}
	if m.config.IOSBundleID != "" {
		testsEnv = append(testsEnv, EnvVar{Name: "IOS_BUNDLE_ID", Value: m.config.IOSBundleID})
	}
	containers = append(containers, Container{
		Name:         "tests",
		Image:        m.config.IOSTestImage,
		Command:      []string{"sh", "-c"},
		Args:         []string{testsScript},
		Env:          testsEnv,
		VolumeMounts: []VolumeMount{{Name: "shared", MountPath: "/shared"}},
	})

	gracePeriod := int64(5) // Kill sidecar containers (hub-client) quickly after tests finish.
	pod := &Pod{
		Metadata: ObjectMeta{
			Name:      podName,
			Namespace: m.client.Namespace,
			Labels: map[string]string{
				labelManaged:  "true",
				labelDevice:   dev.Serial,
				labelPlatform: dev.Platform,
			},
		},
		Spec: PodSpec{
			RestartPolicy:                 "Never",
			Containers:                    containers,
			Volumes:                       volumes,
			TerminationGracePeriodSeconds: &gracePeriod,
		},
	}

	return m.client.CreatePod(ctx, pod)
}

func (m *Manager) adbEndpoint(serial string) string {
	if v, ok := m.adbEndpoints.Load(serial); ok {
		return v.(string)
	}
	return ""
}

// ── Result processing ─────────────────────────────────────────────────────────

var (
	rePassing   = regexp.MustCompile(`(\d+) passing(?:\s+\((?:(\d+)m\s*)?(\d+(?:\.\d+)?)s\))?`)
	reFailing   = regexp.MustCompile(`(\d+) failing`)
	rePending   = regexp.MustCompile(`(\d+) pending`)
	reSessionMs = regexp.MustCompile(`<-- POST /session \d+ (\d+) ms`)
	reApkMs     = regexp.MustCompile(`\[setup\] apk: (\d+(?:\.\d+)?)s`)
)

type testRunSummary struct {
	Serial     string
	Model      string
	Platform   string // "android" or "ios"
	StartedAt  time.Time
	FinishedAt time.Time
	Passing    int
	Failing    int
	Pending    int
	Found      bool
	TestSecs   float64
	TestLog    []byte
	AppiumLog  []byte
	Screenshot []byte
	BatteryPct  int
	BatteryTemp float64
	RunID       int64
	SessionMs  int
	ApkMs      int
}

func (s testRunSummary) deviceLabel() string {
	if s.Model != "" {
		return fmt.Sprintf("%s (%s)", s.Serial, s.Model)
	}
	return s.Serial
}

func (s testRunSummary) totalDuration() time.Duration {
	if s.StartedAt.IsZero() {
		return 0
	}
	return s.FinishedAt.Sub(s.StartedAt)
}

func (s testRunSummary) setupDuration() time.Duration {
	total := s.totalDuration()
	test := time.Duration(s.TestSecs * float64(time.Second))
	if test > total {
		return 0
	}
	return total - test
}

func (m *Manager) processPodResult(ctx context.Context, serial, model, platform string, pod *Pod) testRunSummary {
	battPct := -1
	var battTemp float64
	if v, ok := m.batteryInfo.Load(serial); ok {
		if arr, ok := v.([2]float64); ok {
			battPct = int(arr[0])
			battTemp = arr[1]
		}
	}
	summary := testRunSummary{
		Serial:      serial,
		Model:       model,
		Platform:    platform,
		FinishedAt:  time.Now(),
		BatteryPct:  battPct,
		BatteryTemp: battTemp,
	}

	// Read tests container logs.
	testLog, err := m.client.PodLogs(ctx, pod.Metadata.Name, "tests", 0)
	if err != nil {
		log.Printf("[k8s] %s: read tests logs: %v", serial, err)
	} else {
		summary.TestLog = testLog
	}

	// Read appium container logs (last 2000 lines).
	appiumLog, err := m.client.PodLogs(ctx, pod.Metadata.Name, "appium", 2000)
	if err == nil {
		summary.AppiumLog = appiumLog
	}

	// Parse wdio output.
	scanner := bufio.NewScanner(bytes.NewReader(summary.TestLog))
	for scanner.Scan() {
		line := scanner.Text()
		if ms := rePassing.FindStringSubmatch(line); ms != nil {
			summary.Passing, _ = strconv.Atoi(ms[1])
			summary.Found = true
			if ms[3] != "" {
				secs, _ := strconv.ParseFloat(ms[3], 64)
				if ms[2] != "" {
					mins, _ := strconv.Atoi(ms[2])
					secs += float64(mins) * 60
				}
				summary.TestSecs = secs
			}
		}
		if ms := reFailing.FindStringSubmatch(line); ms != nil {
			summary.Failing, _ = strconv.Atoi(ms[1])
			summary.Found = true
		}
		if ms := rePending.FindStringSubmatch(line); ms != nil {
			summary.Pending, _ = strconv.Atoi(ms[1])
		}
		if ms := reApkMs.FindStringSubmatch(line); ms != nil {
			secs, _ := strconv.ParseFloat(ms[1], 64)
			summary.ApkMs = int(secs * 1000)
		}
	}

	// Parse Appium session creation time.
	if len(summary.AppiumLog) > 0 {
		for _, line := range strings.Split(string(summary.AppiumLog), "\n") {
			if ms := reSessionMs.FindStringSubmatch(line); ms != nil {
				summary.SessionMs, _ = strconv.Atoi(ms[1])
				break
			}
		}
	}

	sep := strings.Repeat("─", 52)
	log.Printf("[k8s/report] %s", sep)
	if !summary.Found {
		log.Printf("[k8s/report] %s — no results (pod crashed?)", summary.deviceLabel())
	} else {
		verdict := "PASS"
		if summary.Failing > 0 {
			verdict = "FAIL"
		}
		log.Printf("[k8s/report] %s  %s", verdict, summary.deviceLabel())
		log.Printf("[k8s/report]   passing: %d | failing: %d | pending: %d",
			summary.Passing, summary.Failing, summary.Pending)
	}
	log.Printf("[k8s/report] %s", sep)

	// Screenshot on failure.
	if !summary.Found || summary.Failing > 0 {
		png, err := m.takeScreenshot(serial, summary.Platform)
		if err != nil {
			log.Printf("[k8s/screenshot] %s: %v", serial, err)
		} else if len(png) > 0 {
			summary.Screenshot = png
			log.Printf("[k8s/screenshot] captured for %s (%d bytes)", serial, len(png))
		}
	}

	return summary
}

func (m *Manager) saveTestResult(summary testRunSummary) testRunSummary {
	if m.store != nil {
		run := store.Run{
			Serial:       summary.Serial,
			Model:        summary.Model,
			FinishedAt:   summary.FinishedAt,
			Passing:      summary.Passing,
			Failing:      summary.Failing,
			Pending:      summary.Pending,
			Found:        summary.Found,
			TotalSeconds: summary.totalDuration().Seconds(),
			TestSeconds:  summary.TestSecs,
			BatteryPct:  summary.BatteryPct,
			BatteryTemp: summary.BatteryTemp,
			SessionMs:    summary.SessionMs,
			ApkMs:        summary.ApkMs,
		}
		id, err := m.store.Insert(run)
		if err != nil {
			log.Printf("[k8s/report] sqlite insert: %v", err)
		} else {
			summary.RunID = id
			m.saveLogs(id, summary)
			if m.NotifyFn != nil {
				m.NotifyFn()
			}
		}
	}
	m.writeFileReport(summary)
	return summary
}

// takeScreenshot captures a PNG from the device via hub-server REST proxy.
func (m *Manager) takeScreenshot(serial, platform string) ([]byte, error) {
	if m.config.HubURL == "" {
		return nil, fmt.Errorf("HubURL not configured")
	}
	url := fmt.Sprintf("%s/api/v1/device/%s/screenshot", m.config.HubURL, serial)
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// rebootViaHub sends a reboot request through hub-server → peer REST API.
// Android peer uses GET, iOS peer uses POST.
func (m *Manager) rebootViaHub(serial, platform string) error {
	if m.config.HubURL == "" {
		return fmt.Errorf("HubURL not configured")
	}
	url := fmt.Sprintf("%s/api/v1/device/%s/reboot", m.config.HubURL, serial)
	method := http.MethodGet
	if platform == "ios" {
		method = http.MethodPost
	}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, url, err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: status %d", method, url, resp.StatusCode)
	}
	return nil
}

func (m *Manager) rebootAndReport(summary testRunSummary) {
	log.Printf("[k8s/reboot] rebooting %s via hub-server...", summary.deviceLabel())
	if err := m.rebootViaHub(summary.Serial, summary.Platform); err != nil {
		log.Printf("[k8s/reboot] %s: %v", summary.Serial, err)
		m.rebooting.Delete(summary.Serial)
		m.updateBootResult(summary.RunID, 0, false)
		return
	}
	// rebooting flag stays set; Reconcile clears it when device comes back online.
	m.updateBootResult(summary.RunID, 0, true)
}

func (m *Manager) updateBootResult(runID int64, bootDuration time.Duration, bootOK bool) {
	if m.store == nil || runID == 0 {
		return
	}
	if err := m.store.UpdateBoot(runID, bootDuration.Seconds(), bootOK); err != nil {
		log.Printf("[k8s/report] sqlite update boot: %v", err)
	} else if m.NotifyFn != nil {
		m.NotifyFn()
	}
}

func (m *Manager) writeFileReport(summary testRunSummary) {
	if err := os.MkdirAll("reports", 0o755); err != nil {
		return
	}
	filename := fmt.Sprintf("reports/%s.log", summary.FinishedAt.Format("2006-01-02"))
	m.reportMu.Lock()
	defer m.reportMu.Unlock()
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()

	sep := strings.Repeat("─", 60)
	fmt.Fprintln(f, sep)
	fmt.Fprintf(f, "Time:    %s\n", summary.FinishedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(f, "Device:  %s\n", summary.deviceLabel())
	if !summary.Found {
		fmt.Fprintln(f, "Tests:   no results (pod crashed before tests ran)")
	} else {
		verdict := "PASS"
		if summary.Failing > 0 {
			verdict = "FAIL"
		}
		fmt.Fprintf(f, "Tests:   %s  |  passing: %d  failing: %d  pending: %d\n",
			verdict, summary.Passing, summary.Failing, summary.Pending)
	}
	if total := summary.totalDuration(); total > 0 {
		fmt.Fprintf(f, "Timing:  total: %s  |  setup: %s  |  tests: %.1fs\n",
			fmtDuration(total), fmtDuration(summary.setupDuration()), summary.TestSecs)
	}
	fmt.Fprintf(f, "Reboot:  pending...\n")
	fmt.Fprintln(f, sep)
	fmt.Fprintln(f, "")
}

func (m *Manager) saveLogs(runID int64, summary testRunSummary) {
	dir := fmt.Sprintf("reports/logs/%d", runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	hasLogs := false
	if len(summary.TestLog) > 0 {
		if err := os.WriteFile(dir+"/test.log", summary.TestLog, 0o644); err == nil {
			hasLogs = true
		}
	}
	if len(summary.AppiumLog) > 0 {
		_ = os.WriteFile(dir+"/appium.log", summary.AppiumLog, 0o644)
		hasLogs = true
	}
	if hasLogs && m.store != nil {
		_ = m.store.SetHasLogs(runID)
	}
	if len(summary.Screenshot) > 0 {
		if err := os.WriteFile(dir+"/screen.png", summary.Screenshot, 0o644); err == nil {
			if m.store != nil {
				_ = m.store.SetHasScreenshot(runID)
			}
		}
	}
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	mins := int(d.Minutes())
	secs := int(d.Seconds()) % 60
	if secs == 0 {
		return fmt.Sprintf("%dm", mins)
	}
	return fmt.Sprintf("%dm %ds", mins, secs)
}
