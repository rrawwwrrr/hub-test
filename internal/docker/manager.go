package docker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"hub-test/internal/adb"
	"hub-test/internal/store"
	"hub-test/internal/types"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	labelManaged   = "hub-test.managed"
	labelDevice    = "hub-test.device"
	labelPort      = "hub-test.port"
	labelRole      = "hub-test.role"
	labelModel     = "hub-test.model"
	labelStartedAt        = "hub-test.started_at"         // RFC3339 timestamp when test container was created
	labelAppiumStartedAt  = "hub-test.appium_started_at"  // RFC3339 timestamp when appium container was created
	labelBattery          = "hub-test.battery"             // battery % at test start (-1 = unknown)

	roleAppium = "appium"
	roleTests  = "tests"

	// adbNetwork is a dedicated bridge network shared by appium + test containers
	// so they can reach each other by container name without going through the host.
	adbNetwork = "hub-test"
)

// wdio spec-reporter output patterns.
// "4 passing (13.8s)"    → group 1 = count, group 3 = seconds
// "4 passing (1m 13.8s)" → group 1 = count, group 2 = minutes, group 3 = seconds
var (
	rePassing    = regexp.MustCompile(`(\d+) passing(?:\s+\((?:(\d+)m\s*)?(\d+(?:\.\d+)?)s\))?`)
	reFailing    = regexp.MustCompile(`(\d+) failing`)
	rePending    = regexp.MustCompile(`(\d+) pending`)
	reSessionMs  = regexp.MustCompile(`<-- POST /session \d+ (\d+) ms`)
	reApkMs      = regexp.MustCompile(`\[setup\] apk: (\d+(?:\.\d+)?)s`)
)

// Config holds the manager configuration.
type Config struct {
	AppiumImage  string
	TestImage    string // image to use for test containers ("" = disabled)
	BasePort     int
	ADBHost      string // hostname of ADB server, reachable from Appium containers
	ADBPort      int
	APKServeURL  string // HTTP URL of the APK served by the built-in web server
	              //   e.g. "http://localhost:8080/apk/ApiDemos-debug.apk"
	              // Appium containers use --network=host so localhost resolves to the host.
	HubDNS       string // CoreDNS server address for hub mode (e.g. "10.96.0.10")
	K8sNamespace string // Kubernetes namespace for pod orchestration; "" = Docker mode
	K8sConfig    string // path to kubeconfig; "" = in-cluster or $KUBECONFIG
}

// deviceContainers tracks the pair of containers managed for one device.
type deviceContainers struct {
	AppiumID        string
	AppiumPort      int
	AppiumName      string
	AppiumStatus    string    // "running" | "exited" | "created" | ""
	AppiumStartedAt time.Time // when the appium container was started

	TestID        string
	TestStatus    string    // "running" | "exited" | "created" | ""
	TestStartedAt time.Time // when the test container was created
	DeviceModel   string    // ro.product.model, stored in container labels
	BatteryPct    int       // battery level at test start (-1 = unknown)
}

// testRunSummary carries the parsed wdio result for one device/run.
type testRunSummary struct {
	Serial     string
	Model      string
	StartedAt  time.Time // when the test container started
	FinishedAt time.Time
	Passing    int
	Failing    int
	Pending    int
	Found      bool    // false if container crashed before wdio produced any output
	TestSecs   float64 // wdio test execution time in seconds (from "N passing (Xs)")
	TestLog    []byte  // raw wdio output (saved to disk on failure)
	AppiumLog  []byte  // raw appium output (saved to disk on failure)
	Screenshot []byte  // PNG screenshot taken before reboot on failure
	BatteryPct int     // battery level at test start (-1 = unknown)
	UsbPath    string  // sysfs USB path at time of run, e.g. "1-3.2"
	RunID      int64   // DB row ID assigned immediately after test, before reboot
	SessionMs  int     // Appium POST /session duration in ms
	ApkMs      int     // APK install duration in ms (logged by wdio before hook)
}

func (s testRunSummary) deviceLabel() string {
	if s.Model != "" {
		return fmt.Sprintf("%s (%s)", s.Serial, s.Model)
	}
	return s.Serial
}

// totalDuration returns the full container lifetime (setup + tests).
func (s testRunSummary) totalDuration() time.Duration {
	if s.StartedAt.IsZero() {
		return 0
	}
	return s.FinishedAt.Sub(s.StartedAt)
}

// setupDuration returns the time spent on setup (session init + APK install),
// computed as total - test execution.
func (s testRunSummary) setupDuration() time.Duration {
	total := s.totalDuration()
	test := time.Duration(s.TestSecs * float64(time.Second))
	if test > total {
		return 0
	}
	return total - test
}

// RunningDevice is an alias for the shared type.
type RunningDevice = types.RunningDevice

// Manager handles the lifecycle of Appium (and optionally test) Docker containers.
type Manager struct {
	cli       *client.Client
	config    Config
	store     *store.Store
	NotifyFn    func() // called after each run is saved; used for SSE push
	ReconcileFn func() // called when a test container exits; triggers a new reconcile
	rebooting    sync.Map   // serial → struct{}: device is mid-reboot, skip test creation
	reported     sync.Map   // containerID → struct{}: already processed, skip duplicate
	reportMu     sync.Mutex // serialises writes to the daily report file
	usbCache     sync.Map   // serial → [3]string{path, vid, pid}: last known USB info
	adbEndpoints sync.Map   // serial → "host:port": per-device ADB endpoint (hub mode)
}

// SetNotify sets the function called after each run is saved.
func (m *Manager) SetNotify(fn func()) { m.NotifyFn = fn }

// SetReconcile sets the function called when a test container exits.
func (m *Manager) SetReconcile(fn func()) { m.ReconcileFn = fn }

// NewManager creates a new Manager. st may be nil (SQLite disabled).
func NewManager(cli *client.Client, cfg Config, st *store.Store) *Manager {
	m := &Manager{cli: cli, config: cfg, store: st}
	// Pre-populate usbCache from the most recent device_events so that
	// usb_path is available even after a redeploy without a reconnect event.
	if st != nil {
		if events, err := st.LatestUSBInfoPerDevice(); err == nil {
			for _, e := range events {
				m.usbCache.Store(e.Serial, [3]string{e.USBPath, e.VID, e.PID})
			}
		}
	}
	return m
}

// adbEndpoint returns the ADB endpoint "host:port" for the given serial,
// or "" if no override is set (local ADB mode).
func (m *Manager) adbEndpoint(serial string) string {
	if v, ok := m.adbEndpoints.Load(serial); ok {
		return v.(string)
	}
	return ""
}

// SetUSBInfo updates usbCache for a device. Called by the USB monitor on mode_change.
func (m *Manager) SetUSBInfo(serial, path, vid, pid string) {
	if serial != "" {
		m.usbCache.Store(serial, [3]string{path, vid, pid})
	}
}

// vidPidOK returns true if the device's current VID:PID matches its configured
// test VID:PID, or if no config is set.
func (m *Manager) vidPidOK(serial string, configs map[string]store.DeviceConfig) bool {
	cfg, ok := configs[serial]
	if !ok || cfg.TestVID == "" {
		return true
	}
	cached, ok := m.usbCache.Load(serial)
	if !ok {
		return false
	}
	info := cached.([3]string)
	return strings.EqualFold(info[1], cfg.TestVID) && strings.EqualFold(info[2], cfg.TestPID)
}

// RunningDevices returns current Appium + test container state for all managed devices.
func (m *Manager) RunningDevices(ctx context.Context) []RunningDevice {
	managed, err := m.listManaged(ctx)
	if err != nil {
		return nil
	}
	out := make([]RunningDevice, 0, len(managed))
	for serial, dc := range managed {
		if dc.AppiumStatus != "running" {
			continue
		}
		out = append(out, RunningDevice{
			Serial:          serial,
			AppiumPort:      dc.AppiumPort,
			AppiumStartedAt: dc.AppiumStartedAt,
			TestStartedAt:   dc.TestStartedAt,
			HasTest:         dc.TestStatus == "running",
		})
	}
	return out
}

// Reconcile brings the set of running containers in sync with connected ADB devices.
func (m *Manager) Reconcile(ctx context.Context, devices []adb.Device) error {
	if err := m.ensureNetwork(ctx); err != nil {
		return fmt.Errorf("ensure network: %w", err)
	}

	existing, err := m.listManaged(ctx)
	if err != nil {
		return fmt.Errorf("list managed containers: %w", err)
	}

	// Load per-device test VID:PID configs once per cycle.
	var devConfigs map[string]store.DeviceConfig
	if m.store != nil {
		if dc, err := m.store.AllDeviceConfigs(); err == nil {
			devConfigs = dc
		}
	}
	if devConfigs == nil {
		devConfigs = make(map[string]store.DeviceConfig)
	}

	readyDevices := make(map[string]adb.Device)
	for _, d := range devices {
		// Update per-device ADB endpoint (hub mode); clear for local-mode devices.
		if d.ADBHost != "" {
			m.adbEndpoints.Store(d.Serial, fmt.Sprintf("%s:%d", d.ADBHost, d.ADBPort))
		} else {
			m.adbEndpoints.Delete(d.Serial)
		}
		if d.IsReady() {
			readyDevices[d.Serial] = d
		}
	}

	// Remove containers for disconnected devices.
	for serial, dc := range existing {
		if _, connected := readyDevices[serial]; !connected {
			if !m.vidPidOK(serial, devConfigs) {
				log.Printf("[skip] %s disconnected but VID:PID mismatch — likely mode change, keeping containers", serial)
				continue
			}
			log.Printf("[remove] device %s disconnected", serial)
			m.logEvent(serial, dc.DeviceModel, "disconnected")
			m.removeDevice(ctx, serial, dc)
		}
	}

	// Create containers for new devices.
	for serial, dev := range readyDevices {
		dc, running := existing[serial]

		// --- Appium container ---
		appiumExited := dc.AppiumID != "" && dc.AppiumStatus != "running"
		isNewDevice := !running // device has no managed containers at all
		if !running || dc.AppiumID == "" || appiumExited {
			if !m.vidPidOK(serial, devConfigs) {
				log.Printf("[skip] device %s connected but VID:PID mismatch (wrong mode), not creating containers", serial)
				continue
			}
			if appiumExited {
				log.Printf("[restart] appium exited for %s, removing and restarting", serial)
				// Remove the exited appium container/pod before creating a new one.
				if m.isK8sMode() {
					_ = m.removePodK8s(ctx, dc.TestID)
					dc.TestID = ""
					dc.TestStatus = ""
				} else {
					_ = m.cli.ContainerRemove(ctx, dc.AppiumID, container.RemoveOptions{Force: true})
				}
				dc.AppiumID = ""
				dc.AppiumStatus = ""
			}

			port := dc.AppiumPort
			if port == 0 {
				port = m.nextPort(existing)
			}
			if m.isK8sMode() {
				log.Printf("[create] k8s pod for device %s", serial)
			} else {
				log.Printf("[create] appium for device %s on host port %d", serial, port)
			}

			newDC, err := m.createAppium(ctx, dev, port)
			if err != nil {
				log.Printf("[create] appium for %s: %v", serial, err)
				continue
			}
			m.recordContainerEvent(serial, "appium_create", newDC.AppiumName)
			// Update Appium fields; in K8s mode also copy TestID (both in same pod).
			dc.AppiumID = newDC.AppiumID
			dc.AppiumPort = newDC.AppiumPort
			dc.AppiumName = newDC.AppiumName
			dc.AppiumStatus = newDC.AppiumStatus
			dc.DeviceModel = newDC.DeviceModel
			if m.isK8sMode() {
				dc.TestID = newDC.TestID
				dc.TestStatus = newDC.TestStatus
				dc.TestStartedAt = newDC.TestStartedAt
				dc.BatteryPct = newDC.BatteryPct
			}
			existing[serial] = dc

			// Log connect event only for newly appearing devices, not Appium restarts.
			if isNewDevice {
				m.logEvent(serial, dev.Model, "connected")
			}
		} else {
			log.Printf("[skip] appium already running for %s (port %d)", serial, dc.AppiumPort)
		}

		// --- Test container (optional) ---
		if m.config.TestImage == "" {
			continue
		}

		if dc.TestID != "" && dc.TestStatus == "running" {
			log.Printf("[skip] tests already running for %s", serial)
			continue
		}

		// Remove a stopped test container/pod, report results, then reboot device.
		if dc.TestID != "" {
			if _, alreadyDone := m.reported.LoadOrStore(dc.TestID, struct{}{}); alreadyDone {
				// Already reported but container/pod still present — remove it.
				log.Printf("[skip] already reported test container for %s, removing stale container", serial)
				if m.isK8sMode() {
					_ = m.removePodK8s(ctx, dc.TestID)
				} else {
					_ = m.cli.ContainerRemove(ctx, dc.TestID, container.RemoveOptions{Force: true})
				}
				continue
			}
			summary := m.reportTestResult(ctx, serial, dc)
			log.Printf("[cleanup] removing stopped test container for %s", serial)
			if m.isK8sMode() {
				_ = m.removePodK8s(ctx, dc.TestID)
			} else {
				_ = m.cli.ContainerRemove(ctx, dc.TestID, container.RemoveOptions{Force: true})
			}
			// Write test results to DB and file immediately — don't wait for reboot.
			summary = m.saveTestResult(summary)
			m.rebooting.Store(serial, struct{}{})
			go m.rebootAndReport(summary)
			continue // test container will be recreated after device comes back
		}

		// Don't start tests while the device is rebooting.
		if _, isRebooting := m.rebooting.Load(serial); isRebooting {
			log.Printf("[skip] device %s is rebooting", serial)
			continue
		}

		log.Printf("[create] test container for device %s → appium host.docker.internal:%d", serial, dc.AppiumPort)
		if err := m.createTest(ctx, dev, dc.AppiumPort); err != nil {
			log.Printf("[create] test container for %s: %v", serial, err)
		} else {
			m.recordContainerEvent(serial, "test_create", "tests-"+serial)
		}
	}

	return nil
}

// PullImage pulls the given image, streaming progress to stdout.
func (m *Manager) PullImage(ctx context.Context, img string) error {
	log.Printf("Pulling image %s ...", img)
	rc, err := m.cli.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("image pull %s: %w", img, err)
	}
	defer rc.Close()
	_, err = io.Copy(os.Stdout, rc)
	return err
}

// BuildTestImage builds the test Docker image from the given context directory.
func (m *Manager) BuildTestImage(ctx context.Context, contextDir, tag string) error {
	log.Printf("Building test image %s from %s ...", tag, contextDir)
	// Use docker CLI for simplicity (avoids streaming tar complexity in SDK).
	// A full SDK build is possible but verbose for this use-case.
	return runCmd(ctx, "docker", "build", "-t", tag, contextDir)
}

// ── internal helpers ──────────────────────────────────────────────────────────

func (m *Manager) ensureNetwork(ctx context.Context) error {
	if m.isK8sMode() {
		return nil // K8s pods have networking by default
	}
	f := filters.NewArgs(filters.Arg("name", adbNetwork))
	nets, err := m.cli.NetworkList(ctx, network.ListOptions{Filters: f})
	if err != nil {
		return err
	}
	for _, n := range nets {
		if n.Name == adbNetwork {
			return nil // already exists
		}
	}
	log.Printf("Creating Docker network %q", adbNetwork)
	_, err = m.cli.NetworkCreate(ctx, adbNetwork, network.CreateOptions{Driver: "bridge"})
	return err
}

// listManaged returns a map[serial]deviceContainers for all managed containers
// or pods (depending on orchestration mode).
func (m *Manager) listManaged(ctx context.Context) (map[string]deviceContainers, error) {
	if m.isK8sMode() {
		return m.listManagedK8s(ctx)
	}
	f := filters.NewArgs(filters.Arg("label", labelManaged+"=true"))
	list, err := m.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, err
	}

	result := make(map[string]deviceContainers)
	for _, c := range list {
		serial := c.Labels[labelDevice]
		dc := result[serial]

		if model := c.Labels[labelModel]; model != "" {
			dc.DeviceModel = model
		}
		switch c.Labels[labelRole] {
		case roleAppium:
			port, _ := strconv.Atoi(c.Labels[labelPort])
			name := ""
			if len(c.Names) > 0 {
				name = c.Names[0][1:] // strip leading "/"
			}
			dc.AppiumID = c.ID
			dc.AppiumPort = port
			dc.AppiumName = name
			dc.AppiumStatus = c.State // "running", "exited", etc.
			if ts := c.Labels[labelAppiumStartedAt]; ts != "" {
				t, _ := time.Parse(time.RFC3339, ts)
				dc.AppiumStartedAt = t
			}
		case roleTests:
			dc.TestID = c.ID
			dc.TestStatus = c.State // "running", "exited", etc.
			if ts := c.Labels[labelStartedAt]; ts != "" {
				t, _ := time.Parse(time.RFC3339, ts)
				dc.TestStartedAt = t
			}
			dc.BatteryPct = -1
			if bp := c.Labels[labelBattery]; bp != "" {
				n, _ := strconv.Atoi(bp)
				dc.BatteryPct = n
			}
		}
		result[serial] = dc
	}
	return result, nil
}

// nextPort returns the lowest port >= BasePort not already used.
func (m *Manager) nextPort(existing map[string]deviceContainers) int {
	used := make(map[int]bool, len(existing))
	for _, dc := range existing {
		if dc.AppiumPort > 0 {
			used[dc.AppiumPort] = true
		}
	}
	port := m.config.BasePort
	for used[port] {
		port++
	}
	return port
}

// createAppium starts an Appium container for the given device.
//
// The container runs with --network=host so that `adb forward` port bindings
// created by Appium on the host are accessible via localhost inside the
// container. Without host networking, Appium would forward device ports on the
// host but then fail to connect to them because "localhost" inside the
// container is the container itself, not the host.
func (m *Manager) createAppium(ctx context.Context, dev adb.Device, hostPort int) (*deviceContainers, error) {
	if m.isK8sMode() {
		return m.createPodK8s(ctx, dev)
	}

	name := "appium-" + sanitize(dev.Serial)

	// Free the port from any orphaned process before creating the container.
	// With --network=host, a previously force-removed container's Appium process
	// may still be alive and holding the port; kill it now so the bind succeeds.
	killPortHolder(hostPort)

	// In hub mode, Appium must connect to the peer pod's ADB server instead
	// of the local one. Per-device overrides take precedence over global config.
	adbAddress := "localhost"
	adbPort := m.config.ADBPort
	if dev.ADBHost != "" {
		adbAddress = dev.ADBHost
	} else if m.config.ADBHost != "" {
		adbAddress = m.config.ADBHost
	}
	if dev.ADBPort != 0 {
		adbPort = dev.ADBPort
	}

	cfg := &container.Config{
		Image: m.config.AppiumImage,
		Env: []string{
			"ANDROID_SERIAL=" + dev.Serial,
			"ANDROID_ADB_SERVER_ADDRESS=" + adbAddress,
			fmt.Sprintf("ANDROID_ADB_SERVER_PORT=%d", adbPort),
		},
		// Skip start.sh (which wraps appium in xvfb-run) — Xvfb is not needed
		// for Android/UiAutomator2, which talks to the device over ADB, not X11.
		// The image ENTRYPOINT is ["sh","-c"], so Cmd[0] is the shell script.
		Cmd: []string{fmt.Sprintf(
			`appium --log /var/log/appium.log --log-timestamp --log-no-colors --port %d --address 0.0.0.0 --allow-insecure=uiautomator2:adb_shell`,
			hostPort,
		)},
		Labels: map[string]string{
			labelManaged:         "true",
			labelDevice:          dev.Serial,
			labelRole:            roleAppium,
			labelPort:            strconv.Itoa(hostPort),
			labelModel:           dev.Model,
			labelAppiumStartedAt: time.Now().UTC().Format(time.RFC3339),
		},
	}

	hostCfg := &container.HostConfig{
		// Host networking: the container shares the host's network stack.
		// adb forward ports appear on 127.0.0.1 and Appium can reach them.
		// It also means Appium can reach the built-in HTTP server on localhost
		// to download the APK (no volume mounts needed).
		NetworkMode: "host",
	}
	// In hub mode, add CoreDNS so the container can resolve cluster hostnames
	// (e.g. peer-{serial}.peer.svc.cluster.local) for ADB connectivity.
	if m.config.HubDNS != "" {
		hostCfg.DNS = []string{m.config.HubDNS}
	}

	resp, err := m.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, name)
	if err != nil {
		// If the container already exists (conflict), inspect it and reuse.
		if strings.Contains(err.Error(), "Conflict") {
			containers, lerr := m.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: filters.NewArgs(filters.Arg("name", "/"+name))})
			if lerr == nil && len(containers) > 0 {
				c := containers[0]
				port := hostPort
				if p, e := strconv.Atoi(c.Labels[labelPort]); e == nil {
					port = p
				}
				log.Printf("[appium] reusing existing %s (id=%s) → port %d", name, c.ID[:12], port)
				return &deviceContainers{
					AppiumID:     c.ID,
					AppiumPort:   port,
					AppiumName:   name,
					AppiumStatus: c.State,
					DeviceModel:  dev.Model,
				}, nil
			}
		}
		return nil, fmt.Errorf("create: %w", err)
	}
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		_ = m.cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return nil, fmt.Errorf("start: %w", err)
	}

	now := time.Now().UTC()
	log.Printf("[appium] started %s (id=%s, host-network) → port %d", name, resp.ID[:12], hostPort)
	return &deviceContainers{
		AppiumID:        resp.ID,
		AppiumPort:      hostPort,
		AppiumName:      name,
		AppiumStatus:    "running",
		AppiumStartedAt: now,
		DeviceModel:     dev.Model,
	}, nil
}

// createTest starts a test container that connects to Appium running on the host network.
// In K8s mode, this is a no-op: the test container is already part of the pod
// created by createAppium/createPodK8s.
func (m *Manager) createTest(ctx context.Context, dev adb.Device, appiumPort int) error {
	if m.isK8sMode() {
		return nil // tests container launched together with appium in createPodK8s
	}
	name := "tests-" + sanitize(dev.Serial)
	now := time.Now().UTC()

	// Remove any orphaned container with the same name (can happen after
	// process restarts when the old container was not cleaned up properly).
	// Errors are ignored — the container may not exist.
	_ = m.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})

	// Grant Appium overlay permissions — suppresses the "display over other
	// apps" dialog that can block tests. Best-effort: silently skipped if
	// Appium hasn't installed its helper packages yet (first-ever run).
	adb.GrantAppiumPermissions(dev.Serial, m.adbEndpoint(dev.Serial))

	// Check battery level before starting tests — skip if below 29%.
	batt := -1
	if level, err := adb.BatteryLevel(dev.Serial, m.adbEndpoint(dev.Serial)); err != nil {
		log.Printf("[battery] %s: %v", dev.Serial, err)
	} else {
		batt = level
		log.Printf("[battery] %s: %d%%", dev.Serial, batt)
		if batt < 29 {
			log.Printf("[skip] %s battery too low (%d%% < 29%%), not starting tests", dev.Serial, batt)
			return fmt.Errorf("battery too low: %d%% (minimum 29%%)", batt)
		}
	}

	env := []string{
		"ANDROID_SERIAL=" + dev.Serial,
		// Appium runs on host network → reach it via host.docker.internal.
		"APPIUM_HOST=host.docker.internal",
		fmt.Sprintf("APPIUM_PORT=%d", appiumPort),
	}

	// Pass the APK URL so wdio.conf.js tells Appium where to fetch the APK.
	// Appium runs with --network=host and downloads the APK from the built-in
	// HTTP server (localhost) without any internet access.
	if m.config.APKServeURL != "" {
		env = append(env, "APIDEMOS_APK_URL="+m.config.APKServeURL)
	}

	cfg := &container.Config{
		Image: m.config.TestImage,
		Env:   env,
		Labels: map[string]string{
			labelManaged:   "true",
			labelDevice:    dev.Serial,
			labelRole:      roleTests,
			labelModel:     dev.Model,
			labelStartedAt: now.Format(time.RFC3339),
			labelBattery:   strconv.Itoa(batt),
		},
	}

	hostCfg := &container.HostConfig{
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
	}

	resp, err := m.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, name)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		_ = m.cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return fmt.Errorf("start: %w", err)
	}

	log.Printf("[tests] started %s (id=%s) → appium host.docker.internal:%d", name, resp.ID[:12], appiumPort)

	// When the container exits, trigger reconcile so results are processed immediately.
	if m.ReconcileFn != nil {
		id := resp.ID
		fn := m.ReconcileFn
		go func() {
			waitC, errC := m.cli.ContainerWait(context.Background(), id, container.WaitConditionNotRunning)
			select {
			case <-waitC:
			case <-errC:
			}
			fn()
		}()
	}

	return nil
}

// removeDevice force-removes both containers for a device (Docker mode) or
// deletes the pod (K8s mode).
func (m *Manager) removeDevice(ctx context.Context, serial string, dc deviceContainers) {
	if m.isK8sMode() {
		if dc.TestID != "" {
			if err := m.removePodK8s(ctx, dc.TestID); err != nil {
				log.Printf("[remove] pod %s: %v", dc.TestID, err)
			}
		}
		if serial != "" {
			m.recordContainerEvent(serial, "container_remove", dc.AppiumName)
		}
		return
	}
	for _, id := range []string{dc.TestID, dc.AppiumID} {
		if id == "" {
			continue
		}
		if err := m.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: true}); err != nil {
			log.Printf("[remove] container %s: %v", id[:12], err)
		}
	}
	if serial != "" {
		m.recordContainerEvent(serial, "container_remove", dc.AppiumName)
	}
}

// reportTestResult reads the logs of a finished test container, parses the
// wdio spec-reporter summary, logs it to stdout, and returns a testRunSummary
// for the file report (written later, after the device reboots).
func (m *Manager) reportTestResult(ctx context.Context, serial string, dc deviceContainers) testRunSummary {
	var usbPath string
	if cached, ok := m.usbCache.Load(serial); ok {
		usbPath = cached.([3]string)[0]
	}
	summary := testRunSummary{
		Serial:     serial,
		Model:      dc.DeviceModel,
		StartedAt:  dc.TestStartedAt,
		FinishedAt: time.Now(),
		BatteryPct: dc.BatteryPct,
		UsbPath:    usbPath,
	}

	var buf bytes.Buffer
	if m.isK8sMode() {
		// K8s: fetch logs directly from the pod's containers via kubectl.
		testLogs, err := m.podLogsK8s(ctx, dc.TestID, "tests")
		if err != nil {
			log.Printf("[report] %s: could not read test logs: %v", serial, err)
		} else {
			buf.Write(testLogs)
		}
		summary.TestLog = buf.Bytes()

		if appLogs, err := m.podLogsK8s(ctx, dc.TestID, "appium"); err == nil {
			summary.AppiumLog = appLogs
		}
	} else {
		// Docker: logs are multiplexed (stdout/stderr); stdcopy demuxes them.
		rc, err := m.cli.ContainerLogs(ctx, dc.TestID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
		})
		if err != nil {
			log.Printf("[report] %s: could not read logs: %v", serial, err)
			return summary
		}
		defer rc.Close()

		if _, err := stdcopy.StdCopy(&buf, &buf, rc); err != nil {
			_, _ = io.Copy(&buf, rc)
		}
		summary.TestLog = buf.Bytes()

		// Capture Appium container logs (last 2000 lines).
		if dc.AppiumID != "" {
			arc, err := m.cli.ContainerLogs(ctx, dc.AppiumID, container.LogsOptions{
				ShowStdout: true,
				ShowStderr: true,
				Tail:       "2000",
			})
			if err == nil {
				var abuf bytes.Buffer
				if _, err := stdcopy.StdCopy(&abuf, &abuf, arc); err != nil {
					_, _ = io.Copy(&abuf, arc)
				}
				arc.Close()
				summary.AppiumLog = abuf.Bytes()
			}
		}
	}

	scanner := bufio.NewScanner(&buf)
	for scanner.Scan() {
		line := scanner.Text()
		if ms := rePassing.FindStringSubmatch(line); ms != nil {
			summary.Passing, _ = strconv.Atoi(ms[1])
			summary.Found = true
			// ms[2] = minutes (optional), ms[3] = seconds
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
		// [setup] apk: 5.3s — logged by wdio before() hook
		if ms := reApkMs.FindStringSubmatch(line); ms != nil {
			secs, _ := strconv.ParseFloat(ms[1], 64)
			summary.ApkMs = int(secs * 1000)
		}
	}

	// Parse Appium session creation time from appium log.
	// Pattern: "<-- POST /session 200 4894 ms"
	if len(summary.AppiumLog) > 0 {
		for _, line := range strings.Split(string(summary.AppiumLog), "\n") {
			if ms := reSessionMs.FindStringSubmatch(line); ms != nil {
				summary.SessionMs, _ = strconv.Atoi(ms[1])
				break
			}
		}
	}

	// Take a screenshot before the device is rebooted (only on failure/crash).
	if !summary.Found || summary.Failing > 0 {
		if png, err := adb.TakeScreenshot(serial, m.adbEndpoint(serial)); err != nil {
			log.Printf("[screenshot] %s: %v", serial, err)
		} else if len(png) > 0 {
			summary.Screenshot = png
			log.Printf("[screenshot] captured for %s (%d bytes)", serial, len(png))
		}
	}

	sep := strings.Repeat("─", 52)
	log.Printf("[report] %s", sep)
	if !summary.Found {
		log.Printf("[report] %s — no results (container crashed?)", summary.deviceLabel())
	} else {
		verdict := "PASS"
		if summary.Failing > 0 {
			verdict = "FAIL"
		}
		log.Printf("[report] %s  %s", verdict, summary.deviceLabel())
		log.Printf("[report]        passing: %d | failing: %d | pending: %d",
			summary.Passing, summary.Failing, summary.Pending)
		if total := summary.totalDuration(); total > 0 {
			log.Printf("[report]        total: %s  setup: %s  tests: %.1fs",
				total.Round(time.Second),
				summary.setupDuration().Round(time.Second),
				summary.TestSecs)
		}
	}
	log.Printf("[report] %s", sep)
	return summary
}

// rebootAndReport reboots the device, waits for it to come back, then updates
// the already-inserted DB row with boot timing.
func (m *Manager) rebootAndReport(summary testRunSummary) {
	log.Printf("[reboot] rebooting %s...", summary.deviceLabel())

	endpoint := m.adbEndpoint(summary.Serial)
	if err := adb.Reboot(summary.Serial, endpoint); err != nil {
		log.Printf("[reboot] %s: %v", summary.Serial, err)
		m.rebooting.Delete(summary.Serial)
		m.updateBootResult(summary.RunID, 0, false)
		return
	}

	bootDuration, err := adb.WaitForReady(summary.Serial, 5*time.Minute, endpoint)
	if err != nil {
		log.Printf("[reboot] %s: %v", summary.Serial, err)
		m.rebooting.Delete(summary.Serial)
		m.updateBootResult(summary.RunID, bootDuration, false)
		return
	}

	log.Printf("[reboot] %s ready after %s", summary.deviceLabel(), bootDuration.Round(time.Second))
	adb.GrantAppiumPermissions(summary.Serial, endpoint)
	// Wait for the device to fully stabilize: some devices switch USB VID:PID
	// after the initial ADB ready signal (e.g. 18d1→22d9 on Realme), briefly
	// dropping off ADB. Without this pause, the next Appium session fails with
	// "device not in the list of connected devices".
	log.Printf("[reboot] %s stabilising (15s)...", summary.deviceLabel())
	time.Sleep(15 * time.Second)
	m.updateBootResult(summary.RunID, bootDuration, true)
	// Clear rebooting BEFORE triggering reconcile so the reconcile sees the
	// device as ready. If we deferred this, the signal could be processed
	// while rebooting was still set and the test creation would be skipped.
	m.rebooting.Delete(summary.Serial)
	// Trigger reconcile so tests start immediately without waiting for a
	// track-devices event (device state is already stable at this point).
	if m.ReconcileFn != nil {
		m.ReconcileFn()
	}
}

// fmtDuration formats a duration as "1m 5s" or "45s".
func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	if s == 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%dm %ds", m, s)
}

// updateBootResult updates boot_ok and boot_seconds on an existing run row.
func (m *Manager) updateBootResult(runID int64, bootDuration time.Duration, bootOK bool) {
	if m.store == nil || runID == 0 {
		return
	}
	if err := m.store.UpdateBoot(runID, bootDuration.Seconds(), bootOK); err != nil {
		log.Printf("[report] sqlite update boot: %v", err)
	} else if m.NotifyFn != nil {
		m.NotifyFn()
	}
}

// saveTestResult writes test results to the DB and file immediately after the
// test finishes, without waiting for the device to reboot.
// Returns the updated summary with RunID filled in.
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
			BatteryPct:   summary.BatteryPct,
			UsbPath:      summary.UsbPath,
			SessionMs:    summary.SessionMs,
			ApkMs:        summary.ApkMs,
		}
		id, err := m.store.Insert(run)
		if err != nil {
			log.Printf("[report] sqlite insert: %v", err)
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

// writeFileReport appends a structured entry to reports/YYYY-MM-DD.log.
func (m *Manager) writeFileReport(summary testRunSummary) {
	if err := os.MkdirAll("reports", 0o755); err != nil {
		log.Printf("[report] mkdir reports: %v", err)
		return
	}

	filename := fmt.Sprintf("reports/%s.log", summary.FinishedAt.Format("2006-01-02"))

	m.reportMu.Lock()
	defer m.reportMu.Unlock()

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("[report] open %s: %v", filename, err)
		return
	}
	defer f.Close()

	sep := strings.Repeat("─", 60)
	fmt.Fprintln(f, sep)
	fmt.Fprintf(f, "Time:    %s\n", summary.FinishedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(f, "Device:  %s\n", summary.deviceLabel())
	if summary.BatteryPct >= 0 {
		fmt.Fprintf(f, "Battery: %d%%\n", summary.BatteryPct)
	}

	if !summary.Found {
		fmt.Fprintln(f, "Tests:   no results (container crashed before tests ran)")
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
			fmtDuration(total),
			fmtDuration(summary.setupDuration()),
			summary.TestSecs)
	}

	fmt.Fprintf(f, "Reboot:  pending...\n")
	fmt.Fprintln(f, sep)
	fmt.Fprintln(f, "")
	log.Printf("[report] written to %s", filename)
}

// saveLogs writes test and appium logs to reports/logs/<id>/ and marks the
// run in the database as having logs available.
func (m *Manager) saveLogs(runID int64, summary testRunSummary) {
	dir := fmt.Sprintf("reports/logs/%d", runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[logs] mkdir %s: %v", dir, err)
		return
	}
	wrote := false
	if len(summary.TestLog) > 0 {
		if err := os.WriteFile(dir+"/test.log", summary.TestLog, 0o644); err != nil {
			log.Printf("[logs] write test.log: %v", err)
		} else {
			wrote = true
		}
	}
	if len(summary.AppiumLog) > 0 {
		if err := os.WriteFile(dir+"/appium.log", summary.AppiumLog, 0o644); err != nil {
			log.Printf("[logs] write appium.log: %v", err)
		} else {
			wrote = true
		}
	}
	if len(summary.Screenshot) > 0 {
		if err := os.WriteFile(dir+"/screen.png", summary.Screenshot, 0o644); err != nil {
			log.Printf("[logs] write screen.png: %v", err)
		} else {
			if err := m.store.SetHasScreenshot(runID); err != nil {
				log.Printf("[logs] set has_screenshot: %v", err)
			}
		}
	}
	if wrote {
		if err := m.store.SetHasLogs(runID); err != nil {
			log.Printf("[logs] set has_logs: %v", err)
		}
		log.Printf("[logs] saved to %s/", dir)
	}
}

// logEvent records a device connect/disconnect event with USB metadata.
// For "connected" events the USB info is read live and cached.
// For "disconnected" events the last cached info is used (device may be gone).
func (m *Manager) logEvent(serial, model, event string) {
	var path, vid, pid string
	if event == "connected" {
		path, vid, pid = adb.USBInfo(serial)
		m.usbCache.Store(serial, [3]string{path, vid, pid})
		log.Printf("[event] %s %s (usb=%s vid=%s pid=%s)", serial, event, path, vid, pid)
	} else {
		if cached, ok := m.usbCache.Load(serial); ok {
			info := cached.([3]string)
			path, vid, pid = info[0], info[1], info[2]
		}
		log.Printf("[event] %s %s", serial, event)
	}
	if m.store == nil {
		return
	}
	_ = m.store.InsertEvent(store.DeviceEvent{
		Serial:  serial,
		Model:   model,
		Event:   event,
		TS:      time.Now().UTC(),
		USBPath: path,
		VID:     vid,
		PID:     pid,
	})
}

// recordContainerEvent writes a container lifecycle event to usb_events.
func (m *Manager) recordContainerEvent(serial, event, detail string) {
	if m.store == nil {
		return
	}
	_ = m.store.InsertUSBEvent(store.USBEvent{
		TS:     time.Now().UTC(),
		Event:  event,
		Serial: serial,
		Detail: detail,
	})
	if m.NotifyFn != nil {
		m.NotifyFn()
	}
}

// sanitize replaces characters not safe for Docker container names with '-'.
func sanitize(s string) string {
	b := []byte(s)
	for i, c := range b {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '.') {
			b[i] = '-'
		}
	}
	return string(b)
}

// ── Kubernetes pod orchestration ──────────────────────────────────────────────
//
// When Config.K8sNamespace is set, the manager creates Kubernetes Pods instead
// of Docker containers. Each device gets one pod (named "hub-test-{serial}")
// with two containers: "appium" (sidecar) and "tests" (main). Both containers
// share localhost within the pod, so tests reach Appium on localhost:4723.
//
// kubectl must be available on PATH and the kubeconfig must grant permissions
// to create/list/delete pods in Config.K8sNamespace.

// isK8sMode reports whether Kubernetes pod orchestration is enabled.
func (m *Manager) isK8sMode() bool { return m.config.K8sNamespace != "" }

// kubectlCmd returns an exec.Cmd for kubectl with optional kubeconfig override
// and the given arguments.
func (m *Manager) kubectlCmd(ctx context.Context, args ...string) *exec.Cmd {
	full := make([]string, 0, len(args)+2)
	if m.config.K8sConfig != "" {
		full = append(full, "--kubeconfig", m.config.K8sConfig)
	}
	full = append(full, args...)
	return exec.CommandContext(ctx, "kubectl", full...)
}

// k8sPodName returns the pod name for the given device serial.
func k8sPodName(serial string) string {
	return "hub-test-" + sanitize(serial)
}

// Minimal JSON structs for parsing kubectl -o json output.
type k8sPodList struct {
	Items []k8sPod `json:"items"`
}

type k8sPod struct {
	Metadata struct {
		Name              string            `json:"name"`
		CreationTimestamp string            `json:"creationTimestamp"`
		Labels            map[string]string `json:"labels"`
	} `json:"metadata"`
	Status struct {
		Phase            string               `json:"phase"`
		ContainerStatuses []k8sContainerStatus `json:"containerStatuses"`
	} `json:"status"`
}

type k8sContainerStatus struct {
	Name  string `json:"name"`
	State struct {
		Running    *struct{ StartedAt string `json:"startedAt"` }               `json:"running"`
		Terminated *struct{ ExitCode int `json:"exitCode"`; FinishedAt string `json:"finishedAt"` } `json:"terminated"`
		Waiting    *struct{ Reason string `json:"reason"` }                     `json:"waiting"`
	} `json:"state"`
}

// listManagedK8s lists all hub-test-managed pods in the K8s namespace.
func (m *Manager) listManagedK8s(ctx context.Context) (map[string]deviceContainers, error) {
	ns := m.config.K8sNamespace
	out, err := m.kubectlCmd(ctx,
		"get", "pods",
		"-n", ns,
		"-l", labelManaged+"=true",
		"-o", "json",
	).Output()
	if err != nil {
		return nil, fmt.Errorf("kubectl get pods: %w", err)
	}

	var list k8sPodList
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, fmt.Errorf("kubectl parse pods: %w", err)
	}

	result := make(map[string]deviceContainers)
	for _, pod := range list.Items {
		serial := pod.Metadata.Labels[labelDevice]
		if serial == "" {
			continue
		}

		podName := pod.Metadata.Name
		model := pod.Metadata.Labels[labelModel]
		batt := -1
		if bp := pod.Metadata.Labels[labelBattery]; bp != "" {
			n, _ := strconv.Atoi(bp)
			batt = n
		}

		var startedAt time.Time
		if ts := pod.Metadata.Labels[labelStartedAt]; ts != "" {
			startedAt, _ = time.Parse(time.RFC3339, ts)
		} else if pod.Metadata.CreationTimestamp != "" {
			startedAt, _ = time.Parse(time.RFC3339, pod.Metadata.CreationTimestamp)
		}

		// Determine status for each container by inspecting containerStatuses.
		testStatus := "running"  // default: still starting or running
		appiumStatus := "running"

		for _, cs := range pod.Status.ContainerStatuses {
			switch cs.Name {
			case "tests":
				if cs.State.Terminated != nil {
					testStatus = "exited"
				} else if cs.State.Running != nil {
					testStatus = "running"
				}
			case "appium":
				if cs.State.Terminated != nil {
					appiumStatus = "exited"
				}
			}
		}
		// If the pod itself has failed (e.g. OOMKilled), treat both as exited.
		if pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded" {
			testStatus = "exited"
			appiumStatus = "exited"
		}

		result[serial] = deviceContainers{
			AppiumID:        podName,
			AppiumPort:      4723,
			AppiumName:      podName,
			AppiumStatus:    appiumStatus,
			AppiumStartedAt: startedAt,
			TestID:          podName,
			TestStatus:      testStatus,
			TestStartedAt:   startedAt,
			DeviceModel:     model,
			BatteryPct:      batt,
		}
	}
	return result, nil
}

// createPodK8s creates a Kubernetes pod for the given device with Appium and
// test containers sharing localhost within the pod.
func (m *Manager) createPodK8s(ctx context.Context, dev adb.Device) (*deviceContainers, error) {
	ns := m.config.K8sNamespace
	podName := k8sPodName(dev.Serial)
	now := time.Now().UTC()

	// Check battery before creating the pod.
	batt := -1
	if level, err := adb.BatteryLevel(dev.Serial, m.adbEndpoint(dev.Serial)); err != nil {
		log.Printf("[battery] %s: %v", dev.Serial, err)
	} else {
		batt = level
		log.Printf("[battery] %s: %d%%", dev.Serial, batt)
		if batt < 29 {
			log.Printf("[skip] %s battery too low (%d%% < 29%%), not creating pod", dev.Serial, batt)
			return nil, fmt.Errorf("battery too low: %d%% (minimum 29%%)", batt)
		}
	}

	adb.GrantAppiumPermissions(dev.Serial, m.adbEndpoint(dev.Serial))

	// Build env vars for the tests container.
	testEnv := []string{
		fmt.Sprintf(`{"name":"ANDROID_SERIAL","value":%q}`, dev.Serial),
		`{"name":"APPIUM_HOST","value":"localhost"}`,
		`{"name":"APPIUM_PORT","value":"4723"}`,
	}
	if m.config.APKServeURL != "" {
		testEnv = append(testEnv, fmt.Sprintf(`{"name":"APIDEMOS_APK_URL","value":%q}`, m.config.APKServeURL))
	}

	appiumCmd := fmt.Sprintf(
		"appium --log /var/log/appium.log --log-timestamp --log-no-colors --port 4723 --address 0.0.0.0 --allow-insecure=uiautomator2:adb_shell",
	)

	// Build pod JSON. We use native sidecar pattern (K8s 1.29+): appium is an
	// initContainer with restartPolicy:Always so the pod completes when only
	// the tests container exits. On older clusters the restartPolicy field is
	// ignored by older API servers and both containers are treated as regular
	// containers (pod won't auto-complete, but we detect test completion via
	// containerStatuses and delete the pod ourselves).
	podJSON := fmt.Sprintf(`{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "name": %q,
    "namespace": %q,
    "labels": {
      %q: "true",
      %q: %q,
      %q: "combined",
      %q: %q,
      %q: %q,
      %q: %q
    }
  },
  "spec": {
    "restartPolicy": "Never",
    "initContainers": [
      {
        "name": "appium",
        "image": %q,
        "restartPolicy": "Always",
        "env": [
          {"name":"ANDROID_SERIAL","value":%q},
          {"name":"ANDROID_ADB_SERVER_ADDRESS","value":%q},
          {"name":"ANDROID_ADB_SERVER_PORT","value":%q}
        ],
        "command": ["sh","-c",%q]
      }
    ],
    "containers": [
      {
        "name": "tests",
        "image": %q,
        "env": [%s]
      }
    ]
  }
}`,
		podName, ns,
		// labels
		labelManaged, labelDevice, dev.Serial, labelRole, labelModel, dev.Model,
		labelStartedAt, now.UTC().Format(time.RFC3339),
		labelBattery, strconv.Itoa(batt),
		// appium container
		m.config.AppiumImage,
		dev.Serial,
		dev.ADBHost,
		strconv.Itoa(dev.ADBPort),
		appiumCmd,
		// tests container
		m.config.TestImage,
		strings.Join(testEnv, ","),
	)

	cmd := m.kubectlCmd(ctx, "apply", "-f", "-", "-n", ns)
	cmd.Stdin = strings.NewReader(podJSON)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("kubectl apply pod: %w\n%s", err, out)
	}
	log.Printf("[k8s] created pod %s in namespace %s", podName, ns)

	return &deviceContainers{
		AppiumID:        podName,
		AppiumPort:      4723,
		AppiumName:      podName,
		AppiumStatus:    "running",
		AppiumStartedAt: now,
		TestID:          podName,
		TestStatus:      "running",
		TestStartedAt:   now,
		DeviceModel:     dev.Model,
		BatteryPct:      batt,
	}, nil
}

// removePodK8s deletes the Kubernetes pod for the given device.
func (m *Manager) removePodK8s(ctx context.Context, podName string) error {
	ns := m.config.K8sNamespace
	out, err := m.kubectlCmd(ctx,
		"delete", "pod", podName,
		"-n", ns,
		"--ignore-not-found",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl delete pod %s: %w\n%s", podName, err, out)
	}
	return nil
}

// podLogsK8s returns the logs of a container within a pod.
func (m *Manager) podLogsK8s(ctx context.Context, podName, containerName string) ([]byte, error) {
	ns := m.config.K8sNamespace
	out, err := m.kubectlCmd(ctx,
		"logs", podName,
		"-c", containerName,
		"-n", ns,
	).Output()
	if err != nil {
		return nil, fmt.Errorf("kubectl logs %s/%s: %w", podName, containerName, err)
	}
	return out, nil
}


// CheckPods is a no-op in Docker mode (no K8s pods).
func (m *Manager) CheckPods(_ context.Context) {}
