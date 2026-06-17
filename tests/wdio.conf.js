// WebdriverIO config for running ApiDemos tests against an external Appium server.
//
// Required environment variables:
//   APPIUM_HOST      – hostname of the Appium server (default: localhost)
//   APPIUM_PORT      – port of the Appium server (default: 4723)
//   ANDROID_SERIAL   – device serial number forwarded to Appium capabilities

const appiumHost = process.env.APPIUM_HOST || 'localhost';
const appiumPort = parseInt(process.env.APPIUM_PORT || '4723', 10);
const deviceSerial = process.env.ANDROID_SERIAL || '';

// APK path inside the Appium container (mounted from host by adbtest).
// Falls back to URL download if the local path is not provided.
const apkApp = process.env.APIDEMOS_APK_PATH ||
  process.env.APIDEMOS_APK_URL ||
  'https://github.com/appium/android-apidemos/releases/download/v6.0.6/ApiDemos-debug.apk';

exports.config = {
  runner: 'local',

  // Connect to the external Appium server (no appium service).
  hostname: appiumHost,
  port: appiumPort,
  path: '/',

  specs: ['./test/specs/**/*.spec.js'],
  maxInstances: 1,

  capabilities: [
    {
      platformName: 'Android',
      'appium:deviceName': deviceSerial,
      'appium:udid': deviceSerial,
      'appium:automationName': 'UiAutomator2',
      'appium:appPackage': 'io.appium.android.apis',
      'appium:appActivity': '.ApiDemos',
      // APK is installed manually in before() so we can time it precisely.
      // autoLaunch=false prevents Appium from trying to start the app on session
      // creation (before the APK is installed).
      'appium:noReset': true,
      'appium:autoLaunch': false,
      'appium:forceAppLaunch': true,
      // Automatically grant all runtime permissions so no permission dialogs block tests.
      'appium:autoGrantPermissions': true,
      'appium:newCommandTimeout': 90,
      'appium:androidInstallTimeout': 120000,
      // UiAutomator2 server install can be slow on first run — give it more time.
      'appium:uiautomator2ServerLaunchTimeout': 60000,
      'appium:uiautomator2ServerInstallTimeout': 60000,
      // Non-rooted devices deny WRITE_SECURE_SETTINGS; ignore that error and continue.
      'appium:ignoreHiddenApiPolicyError': true,
      // Prevent Appium from killing and restarting the ADB server — the ADB server
      // runs inside the peer pod (not locally) and must stay on 0.0.0.0:5037.
      'appium:suppressKillServer': true,
    },
  ],

  framework: 'mocha',
  mochaOpts: {
    timeout: 120000,
  },

  reporters: [
    ['spec', {
      addConsoleLogs: true,
      realtimeReporting: true,
    }],
  ],

  // Log level: trace | debug | info | warn | error | silent
  logLevel: 'info',

  async before() {
    // Install APK and log timing so adbtest can record it.
    const apkStart = Date.now();
    try {
      await driver.installApp(apkApp);
      console.log(`[setup] apk: ${((Date.now() - apkStart) / 1000).toFixed(1)}s`);
    } catch (e) {
      console.warn(`[setup] apk install failed: ${e.message}`);
    }

    // Grant SYSTEM_ALERT_WINDOW to Appium packages from within the session.
    // This runs AFTER Appium has installed/verified its helper apps, so the
    // permission is not lost due to package reinstallation.
    // Requires --allow-insecure=adb_shell on the Appium server.
    const appiumPkgs = [
      'io.appium.settings',
      'io.appium.uiautomator2.server',
      'io.appium.uiautomator2.server.test',
      'io.appium.android.apis',
    ];
    for (const pkg of appiumPkgs) {
      try {
        await driver.execute('mobile: shell', {
          command: 'appops',
          args: ['set', pkg, 'SYSTEM_ALERT_WINDOW', 'allow'],
        });
        console.log(`[before] granted SYSTEM_ALERT_WINDOW to ${pkg}`);
      } catch (e) {
        console.warn(`[before] appops set ${pkg}: ${e.message}`);
      }
    }
    // POST_NOTIFICATIONS (Android 13+): only for packages that declare it
    // in their manifest. Appium internal packages do not declare it, so
    // pm grant throws SecurityException for them — skip them here.
    for (const pkg of ['io.appium.android.apis']) {
      try {
        await driver.execute('mobile: shell', {
          command: 'pm',
          args: ['grant', pkg, 'android.permission.POST_NOTIFICATIONS'],
        });
      } catch (e) { /* pre-Android 13 or permission not declared */ }
    }

    // Wake screen and dismiss keyguard (works only on devices without PIN/password).
    try {
      await driver.execute('mobile: shell', { command: 'input', args: ['keyevent', 'KEYCODE_WAKEUP'] });
      await driver.execute('mobile: shell', { command: 'wm', args: ['dismiss-keyguard'] });
      await driver.pause(500);
    } catch (e) { /* screen already on or keyguard protected */ }

    // Dismiss "USB-подключение" mode-selection dialog if present.
    // NOTE: do NOT call 'svc usb setFunctions mtp' here — it changes the USB PID,
    // causes re-enumeration and drops the ADB connection mid-test.
    try {
      const cancelBtn = await driver.$(
        '//*[@text="ОТМЕНА" or @text="Отмена" or @text="отмена" or ' +
        '@text="CANCEL" or @text="Cancel" or @text="cancel" or ' +
        '@text="DISMISS" or @text="Dismiss" or @text="Закрыть" or @text="ЗАКРЫТЬ"]'
      );
      if (await cancelBtn.isDisplayed()) {
        console.log('[before] dismissing USB dialog via Cancel button');
        await cancelBtn.click();
        await driver.pause(500);
      } else {
        // Fallback to KEYCODE_BACK
        await driver.execute('mobile: shell', { command: 'input', args: ['keyevent', 'KEYCODE_BACK'] });
        await driver.pause(300);
      }
    } catch (e) {
      // No dialog visible — try KEYCODE_BACK just in case
      try {
        await driver.execute('mobile: shell', { command: 'input', args: ['keyevent', 'KEYCODE_BACK'] });
        await driver.pause(300);
      } catch (_) { /* ignore */ }
    }

    // Dismiss any "display over other apps" / permission dialog still visible.
    // Tries both English and Russian button labels used by different ROM versions.
    try {
      const allow = await driver.$(
        '//*[@text="Allow" or @text="Разрешить" or @text="ALLOW" or @text="РАЗРЕШИТЬ" or @text="Да" or @text="ДА"]'
      );
      if (await allow.isDisplayed()) {
        console.log('[before] dismissing permission dialog');
        await allow.click();
        await driver.pause(500);
      }
    } catch (e) { /* no dialog visible, that's fine */ }

    // Press Home to dismiss any remaining system overlays, then bring the app
    // back to the foreground. keycode 3 = KEYCODE_HOME
    try {
      await driver.pressKeyCode(3);
      await driver.activateApp('io.appium.android.apis');
    } catch (e) {
      console.warn('[before] HOME/activate failed:', e.message);
    }

    // Verify the app is actually in the foreground before tests start.
    // Retries up to 3 times with 1s delay — after pm clear / forceAppLaunch
    // the activity may not be ready immediately.
    let activity = '';
    for (let attempt = 1; attempt <= 3; attempt++) {
      activity = await driver.getCurrentActivity();
      console.log(`[before] current activity (attempt ${attempt}): ${activity}`);
      if (activity.includes('io.appium.android.apis')) break;
      if (attempt < 3) {
        await driver.pause(1000);
        await driver.activateApp('io.appium.android.apis');
      }
    }
    if (!activity.includes('io.appium.android.apis')) {
      throw new Error(`ApiDemos is not in foreground after 3 attempts (got: ${activity})`);
    }
  },

  async after() {
    // Appium captures logcat in-memory for the session; dump it into the
    // tests container's stdout (already saved as test.log) so it's not lost
    // once the pod is deleted.
    try {
      const logcat = await driver.getLogs('logcat');
      console.log('===LOGCAT_START===');
      for (const entry of logcat) {
        console.log(entry.message);
      }
      console.log('===LOGCAT_END===');
    } catch (e) {
      console.warn(`[after] could not fetch logcat: ${e.message}`);
    }
  },
};
