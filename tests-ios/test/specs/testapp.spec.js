// Tests for the SwiftUI Counter app.
//
// App structure (ContentView.swift):
//   Image(systemName: "globe")
//   Text("Hello, world!")
//   Text("Счётчик: \(counter)")   — persisted via @AppStorage("counter")
//   Button("Увеличить")           — increments counter by 1

// Helper: element by accessibility label
const byLabel = (label) => $(`~${label}`);

// Helper: static text starting with prefix (XCUITest predicate)
const byTextPrefix = (prefix) =>
  $(`-ios predicate string:label BEGINSWITH "${prefix}"`);

// Read "Счётчик: N" → N
async function getCounter() {
  const el = await byTextPrefix('Счётчик:');
  const text = await el.getText();
  const m = text.match(/Счётчик:\s*(-?\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

describe('CounterApp – Launch', () => {
  before(async () => {
    await driver.pause(1500);
  });

  it('should display "Hello, world!"', async () => {
    const el = await byLabel('Hello, world!');
    await expect(el).toBeDisplayed();
  });

  it('should display the globe image', async () => {
    const el = await byLabel('globe');
    await expect(el).toBeDisplayed();
  });

  it('should display the counter text', async () => {
    const el = await byTextPrefix('Счётчик:');
    await expect(el).toBeDisplayed();
    const text = await el.getText();
    console.log(`[test] counter text: ${text}`);
    expect(text).toMatch(/Счётчик:\s*\d+/);
  });

  it('should display the increment button', async () => {
    const btn = await byLabel('Увеличить');
    await expect(btn).toBeDisplayed();
  });
});

describe('CounterApp – Increment', () => {
  it('should increment counter on single tap', async () => {
    const before = await getCounter();
    console.log(`[test] counter before: ${before}`);

    await (await byLabel('Увеличить')).click();
    await driver.pause(300);

    const after = await getCounter();
    console.log(`[test] counter after: ${after}`);
    expect(after).toBe(before + 1);
  });

  it('should increment counter on multiple taps', async () => {
    const before = await getCounter();
    const btn = await byLabel('Увеличить');

    for (let i = 0; i < 4; i++) {
      await btn.click();
      await driver.pause(150);
    }
    await driver.pause(300);

    const after = await getCounter();
    expect(after).toBe(before + 4);
    console.log(`[test] ${before} → ${after} (+4 taps)`);
  });
});

describe('CounterApp – Persistence', () => {
  it('should persist counter value after app restart', async () => {
    const bundleId = process.env.IOS_BUNDLE_ID || 'com.example.CounterApp';

    await (await byLabel('Увеличить')).click();
    await driver.pause(300);

    const saved = await getCounter();
    console.log(`[test] saved counter: ${saved}`);

    await driver.terminateApp(bundleId);
    await driver.pause(800);
    await driver.activateApp(bundleId);
    await driver.pause(1500);

    const restored = await getCounter();
    console.log(`[test] restored counter: ${restored}`);
    expect(restored).toBe(saved);
  });
});
