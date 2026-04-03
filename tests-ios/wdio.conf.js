// WebdriverIO config for iOS tests via Appium XCUITest.
//
// Required environment variables:
//   APPIUM_HOST   – hostname of the Appium server (default: localhost)
//   APPIUM_PORT   – port of the Appium server (default: 4723)
//   IOS_UDID      – UDID of the iOS device
//   WDA_URL       – URL of the already-running WebDriverAgent, e.g.
//                   http://peer-<serial>.peer.svc.cluster.local:8100
//   IOS_IPA_URL   – URL of the IPA to install (optional; skips install if empty)

const appiumHost = process.env.APPIUM_HOST || 'localhost';
const appiumPort = parseInt(process.env.APPIUM_PORT || '4723', 10);
const udid       = process.env.IOS_UDID || '';
const wdaUrl     = process.env.WDA_URL || '';
const ipaUrl     = process.env.IOS_IPA_URL || '';

exports.config = {
  runner: 'local',

  hostname: appiumHost,
  port: appiumPort,
  path: '/',

  specs: ['./test/specs/**/*.spec.js'],
  maxInstances: 1,

  capabilities: [
    {
      platformName: 'iOS',
      'appium:automationName': 'XCUITest',
      'appium:udid': udid,
      // Use the WDA instance already running inside the peer pod.
      'appium:webDriverAgentUrl': wdaUrl,
      'appium:usePrebuiltWDA': true,
      'appium:skipServerInstallation': true,
      // Install and launch the test app.
      ...(ipaUrl ? { 'appium:app': ipaUrl } : {}),
      'appium:noReset': false,
      'appium:newCommandTimeout': 120,
      'appium:wdaLaunchTimeout': 60000,
      'appium:wdaConnectionTimeout': 60000,
    },
  ],

  framework: 'mocha',
  mochaOpts: {
    timeout: 180000,
  },

  reporters: [
    ['spec', {
      addConsoleLogs: true,
      realtimeReporting: true,
    }],
  ],

  logLevel: 'info',
};
