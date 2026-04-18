package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"hub-test/internal/adb"
	"hub-test/internal/docker"
	k8smgr "hub-test/internal/k8s"
	"hub-test/internal/store"
	"hub-test/internal/types"
	"hub-test/internal/usb"
	"hub-test/internal/web"

	dockerclient "github.com/docker/docker/client"
)

// version is injected at build time via -ldflags "-X main.version=vX.Y.Z".
// Falls back to "dev" for local builds.
var version = "dev"

// Manager is the interface implemented by both docker.Manager and k8s.Manager.
type Manager interface {
	Reconcile(ctx context.Context, devices []adb.Device) error
	CheckPods(ctx context.Context)
	RunningDevices(ctx context.Context) []types.RunningDevice
	BuildTestImage(ctx context.Context, contextDir, tag string) error
	PullImage(ctx context.Context, img string) error
	SetUSBInfo(serial, path, vid, pid string)
	SetNotify(fn func())
	SetReconcile(fn func())
}

// envOr returns the value of the environment variable if set, otherwise the fallback.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envOrInt returns the integer value of the environment variable if set and valid,
// otherwise the fallback.
func envOrInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("Warning: invalid integer value for %s=%q, using default %d", key, v, fallback)
	}
	return fallback
}

// envOrBool returns true if the environment variable is set to "1", "true", or "yes".
func envOrBool(key string) bool {
	switch os.Getenv(key) {
	case "1", "true", "yes":
		return true
	}
	return false
}

// ensureAPK downloads the APK from url to path if the file doesn't already exist.
func ensureAPK(path, url string) error {
	if _, err := os.Stat(path); err == nil {
		log.Printf("[apk] using cached %s", path)
		return nil
	}
	if url == "" {
		return fmt.Errorf("APK not found at %s and no --apk-url provided", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	log.Printf("[apk] downloading %s → %s", url, path)
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write: %w", err)
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	log.Printf("[apk] saved to %s", path)
	return nil
}

// defaultTestImage returns the versioned test image for release builds,
// or empty string for dev builds (tests disabled unless --test-image is passed).
func defaultTestImage() string {
	if version == "dev" {
		return ""
	}
	return "harbor.rnd.lanit.ru/public/hub-test-tests:" + strings.TrimPrefix(version, "v")
}

func main() {
	const defaultAPKURL = "https://github.com/appium/android-apidemos/releases/download/v6.0.6/ApiDemos-debug.apk"

	var (
		watch      = flag.Bool("watch", envOrBool("ADBTEST_WATCH"), "Continuously watch for device changes [$ADBTEST_WATCH]")
		pullImage  = flag.Bool("pull", envOrBool("ADBTEST_PULL"), "Pull Appium image before starting (local mode only) [$ADBTEST_PULL]")
		restartADB = flag.Bool("restart-adb", envOrBool("ADBTEST_RESTART_ADB"), "Restart ADB server to listen on all interfaces [$ADBTEST_RESTART_ADB]")

		appiumImage = flag.String("image", envOr("APPIUM_IMAGE", "appium/appium:latest"), "Appium image [$APPIUM_IMAGE]")
		adbHost     = flag.String("adb-host", envOr("ADB_HOST", "host.docker.internal"), "ADB server host reachable from containers [$ADB_HOST]")

		basePort = flag.Int("port", envOrInt("APPIUM_BASE_PORT", 4723), "Starting host port for Appium containers (local mode) [$APPIUM_BASE_PORT]")
		adbPort  = flag.Int("adb-port", envOrInt("ADB_PORT", 5037), "ADB server port [$ADB_PORT]")

		testImage    = flag.String("test-image", envOr("TEST_IMAGE", defaultTestImage()), "Docker/K8s image for test containers; empty = no tests [$TEST_IMAGE]")
		testBuildCtx = flag.String("test-build", envOr("TEST_BUILD_CONTEXT", ""), "Build test image from this directory before starting [$TEST_BUILD_CONTEXT]")

		apkPath = flag.String("apk", envOr("ADBTEST_APK", "apk/ApiDemos-debug.apk"), "Local APK file path [$ADBTEST_APK]")
		apkURL  = flag.String("apk-url", envOr("ADBTEST_APK_URL", defaultAPKURL), "URL to download APK when --apk file is missing [$ADBTEST_APK_URL]")

		httpAddr = flag.String("http-addr", envOr("ADBTEST_HTTP_ADDR", ":8080"), "HTTP dashboard listen address [$ADBTEST_HTTP_ADDR]")
		dbPath   = flag.String("db", envOr("ADBTEST_DB", "reports/hub-test.db"), "SQLite database path [$ADBTEST_DB]")

		// Hub mode: hub-server WebSocket + K8s pod orchestration.
		hubURL         = flag.String("hub-url", envOr("ADBTEST_HUB_URL", ""), "Hub-server base URL (e.g. http://hub-server.peer.svc.cluster.local:8080) [$ADBTEST_HUB_URL]")
		hubNamespace   = flag.String("hub-namespace", envOr("ADBTEST_HUB_NAMESPACE", "peer"), "K8s namespace for peer pods and test pods [$ADBTEST_HUB_NAMESPACE]")
		hubClientImage       = flag.String("hub-client-image", envOr("ADBTEST_HUB_CLIENT_IMAGE", ""), "hub-client image for ADB tunnel container [$ADBTEST_HUB_CLIENT_IMAGE]")
		hubUsbmuxdHost       = flag.String("hub-usbmuxd-host", envOr("ADBTEST_HUB_USBMUXD_HOST", ""), "USBMUXD_HOST for hub-client (hub-server TCP proxy host) [$ADBTEST_HUB_USBMUXD_HOST]")
		hubUsbmuxdPort       = flag.String("hub-usbmuxd-port", envOr("ADBTEST_HUB_USBMUXD_PORT", "27015"), "USBMUXD_PORT for hub-client (default: 27015) [$ADBTEST_HUB_USBMUXD_PORT]")
		hubHandshakeSecret   = flag.String("hub-handshake-secret", envOr("ADBTEST_HUB_HANDSHAKE_SECRET", ""), "HANDSHAKE_SECRET for hub-client AES-GCM encryption [$ADBTEST_HUB_HANDSHAKE_SECRET]")
		hubAdbTunnelMode     = flag.String("hub-tunnel-mode", envOr("ADBTEST_HUB_TUNNEL_MODE", ""), "TUNNEL_MODE for hub-client: persistent or transient [$ADBTEST_HUB_TUNNEL_MODE]")
		hubNsService   = flag.String("hub-ns-service", envOr("ADBTEST_HUB_NS_SERVICE", "hub-test"), "hub-test K8s service name for APK URL construction [$ADBTEST_HUB_NS_SERVICE]")

		iosTestImage   = flag.String("ios-test-image", envOr("IOS_TEST_IMAGE", ""), "K8s image for iOS test containers; empty = skip iOS devices [$IOS_TEST_IMAGE]")
		iosAppiumImage = flag.String("ios-appium-image", envOr("IOS_APPIUM_IMAGE", ""), "Appium image for iOS (falls back to --image) [$IOS_APPIUM_IMAGE]")
		iosIPAURL      = flag.String("ios-ipa-url", envOr("IOS_IPA_URL", ""), "IPA URL passed to iOS test container as IOS_IPA_URL [$IOS_IPA_URL]")
	)

	intervalDefault := 5 * time.Second
	if v := os.Getenv("ADBTEST_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			intervalDefault = d
		} else {
			log.Printf("Warning: invalid duration for ADBTEST_INTERVAL=%q, using default %s", v, intervalDefault)
		}
	}
	interval := flag.Duration("interval", intervalDefault, "Poll interval in watch mode (local ADB mode) [$ADBTEST_INTERVAL]")

	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmsgprefix)
	log.SetPrefix("hub-test ")

	// Open SQLite store.
	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("Store: %v", err)
	}
	defer st.Close()

	// Ensure APK is available locally (download if needed).
	absAPK, err := filepath.Abs(*apkPath)
	if err != nil {
		log.Fatalf("APK path: %v", err)
	}
	if err := ensureAPK(absAPK, *apkURL); err != nil {
		log.Fatalf("APK: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start HTTP dashboard + APK file server.
	hubSSE := web.NewHub()
	webSrv := web.NewServer(st, hubSSE)
	mux := http.NewServeMux()
	webSrv.RegisterRoutes(mux)
	webSrv.ServeAPKDir(mux, filepath.Dir(absAPK))
	webSrv.ServeLogsDir(mux, "reports/logs")
	httpServer := &http.Server{Addr: *httpAddr, Handler: mux}
	go func() {
		log.Printf("Dashboard listening on http://%s", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server: %v", err)
		}
	}()
	defer httpServer.Shutdown(context.Background())

	// ── Hub mode (K8s) ────────────────────────────────────────────────────────
	if *hubURL != "" {
		log.Printf("Hub mode: hub-server=%s namespace=%s", *hubURL, *hubNamespace)

		k8sClient, err := k8smgr.InClusterClient(*hubNamespace)
		if err != nil {
			log.Fatalf("K8s in-cluster client: %v", err)
		}

		// APK URL inside the cluster via the hub-test Service.
		apkServeURL := fmt.Sprintf("http://%s.%s.svc.cluster.local%s/apk/%s",
			*hubNsService, *hubNamespace, *httpAddr, filepath.Base(absAPK))

		k8sCfg := k8smgr.Config{
			AppiumImage:        *appiumImage,
			TestImage:          *testImage,
			IOSTestImage:       *iosTestImage,
			IOSAppiumImage:     *iosAppiumImage,
			IPAServeURL:        *iosIPAURL,
			HubClientImage:     *hubClientImage,
			HubUsbmuxdHost:     *hubUsbmuxdHost,
			HubUsbmuxdPort:     *hubUsbmuxdPort,
			HubHandshakeSecret: *hubHandshakeSecret,
			AdbTunnelMode:      *hubAdbTunnelMode,
			APKServeURL:        apkServeURL,
			HubURL:             *hubURL,
			Namespace:          *hubNamespace,
		}
		kmgr := k8smgr.NewManager(k8sClient, k8sCfg, st)
		kmgr.NotifyFn = hubSSE.Notify
		webSrv.RunningFn = kmgr.RunningDevices

		log.Printf("Hub mode watch (appium=%s tests=%s namespace=%s)",
			*appiumImage, k8sCfg.TestImage, *hubNamespace)

		// Periodic pod-completion check — collects results from finished pods.
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					kmgr.CheckPods(ctx)
				}
			}
		}()

		// Event-driven lifecycle via WebSocket.
		adb.TrackHubDevices(ctx, *hubURL, *hubNamespace, kmgr, nil)
		log.Println("Shutting down.")
		return
	}

	// ── Local ADB mode (Docker) ────────────────────────────────────────────────
	if *restartADB {
		if err := adb.EnsureServerListensOnAllInterfaces(); err != nil {
			log.Fatalf("Failed to restart ADB server: %v", err)
		}
		time.Sleep(time.Second)
	}

	cli, err := dockerclient.NewClientWithOpts(
		dockerclient.FromEnv,
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		log.Fatalf("Docker client: %v", err)
	}
	defer cli.Close()

	apkServeURL := fmt.Sprintf("http://localhost%s/apk/%s", *httpAddr, filepath.Base(absAPK))

	dockerCfg := docker.Config{
		AppiumImage: *appiumImage,
		TestImage:   *testImage,
		BasePort:    *basePort,
		ADBHost:     *adbHost,
		ADBPort:     *adbPort,
		APKServeURL: apkServeURL,
	}
	dockerMgr := docker.NewManager(cli, dockerCfg, st)

	if *testBuildCtx != "" {
		tag := *testImage
		if tag == "" {
			tag = "hub-test-tests:latest"
			dockerCfg.TestImage = tag
		}
		if err := dockerMgr.BuildTestImage(ctx, *testBuildCtx, tag); err != nil {
			log.Fatalf("Build test image: %v", err)
		}
	}

	if *pullImage {
		if err := dockerMgr.PullImage(ctx, *appiumImage); err != nil {
			log.Fatalf("Pull appium image: %v", err)
		}
		if dockerCfg.TestImage != "" {
			if err := dockerMgr.PullImage(ctx, dockerCfg.TestImage); err != nil {
				log.Fatalf("Pull test image: %v", err)
			}
		}
	}

	var mgr Manager = dockerMgr
	mgr.SetNotify(hubSSE.Notify)

	fetchDevices := func() ([]adb.Device, error) { return adb.ListDevices() }

	reconcileCh := make(chan struct{}, 1)
	go func() {
		for range reconcileCh {
			time.Sleep(300 * time.Millisecond)
			for len(reconcileCh) > 0 {
				<-reconcileCh
			}
			reconcileDevices(ctx, mgr, nil, fetchDevices)
		}
	}()
	mgr.SetReconcile(func() {
		select {
		case reconcileCh <- struct{}{}:
		default:
		}
	})
	webSrv.RunningFn = mgr.RunningDevices

	usbMon := usb.NewMonitor(st)
	usbMon.OnModeChange = mgr.SetUSBInfo

	if *watch {
		log.Printf("Watch mode (interval=%s, appium=%s, tests=%s, base-port=%d)",
			*interval, *appiumImage, dockerCfg.TestImage, *basePort)

		usbTicker := time.NewTicker(*interval)
		defer usbTicker.Stop()
		go func() {
			usbMon.Poll()
			for {
				select {
				case <-ctx.Done():
					return
				case <-usbTicker.C:
					usbMon.Poll()
				}
			}
		}()

		reconcileDevices(ctx, mgr, nil, fetchDevices)

		var prevSnapshot string
		adb.TrackDevices(ctx, func(devices []adb.Device) {
			snap := deviceSnapshot(devices)
			if snap == prevSnapshot {
				return
			}
			prevSnapshot = snap
			reconcileDevices(ctx, mgr, devices, fetchDevices)
		}, func() {
			prevSnapshot = ""
		})

		log.Println("Shutting down.")
	} else {
		usbMon.Poll()
		reconcileDevices(ctx, mgr, nil, fetchDevices)
	}
}

// deviceSnapshot returns a stable string key for a device list.
func deviceSnapshot(devices []adb.Device) string {
	parts := make([]string, len(devices))
	for i, d := range devices {
		parts[i] = d.Serial + "=" + d.State
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func reconcileDevices(ctx context.Context, mgr Manager, devices []adb.Device, fetch func() ([]adb.Device, error)) {
	if devices == nil && fetch != nil {
		var err error
		devices, err = fetch()
		if err != nil {
			log.Printf("list devices: %v", err)
			return
		}
	}
	if devices == nil {
		return
	}
	ready := 0
	for _, d := range devices {
		if d.IsReady() {
			ready++
		}
	}
	log.Printf("Detected %d device(s) total, %d ready.", len(devices), ready)

	if err := mgr.Reconcile(ctx, devices); err != nil {
		log.Printf("Reconcile error: %v", err)
	}
}
