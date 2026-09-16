// Test harness for the calculator's engine.
//
// index.html is a single no-build file: the engine lives in its inline <script>,
// so there is nothing to import. This pulls that script out, evaluates it in a
// node:vm context against a DOM stub just rich enough for the top-level wiring,
// and hands back the pure functions (simulate, xirr, the formatters).
//
// Nothing here touches data/spy-prices.json — the tests run on the committed
// fixture so their expectations never move with the daily data job.

import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));

export const REPO = path.resolve(HERE, '..');
export const INDEX = path.join(REPO, 'index.html');

/** The committed price series every test runs against. */
export const FIXTURE = JSON.parse(
    fs.readFileSync(path.join(HERE, 'fixtures', 'prices.json'), 'utf8'),
);

/** The last line of the boot IIFE's banner. Everything from here on reads
 *  location.search and starts a fetch, so the harness cuts the script above it —
 *  that is how the boot code is made harmless. A rename must fail loudly rather
 *  than silently running the page. */
const BOOT_MARKER = '// Boot — honour a shared plan';

/** The inline <script> of index.html, verbatim. */
export function inlineScript() {
    const html  = fs.readFileSync(INDEX, 'utf8');
    const open  = html.lastIndexOf('<script>');
    const close = html.lastIndexOf('</script>');
    if (open < 0 || close <= open) throw new Error('index.html: inline <script> not found');
    return html.slice(open + '<script>'.length, close);
}

/** The same script with the boot IIFE removed. */
export function engineSource() {
    const script = inlineScript();
    const cut    = script.indexOf(BOOT_MARKER);
    if (cut < 0) throw new Error(`index.html: boot marker "${BOOT_MARKER}" is gone — update tests/harness.mjs`);
    return script.slice(0, cut);
}

/** A DOM node with every property and method the top-level wiring touches. */
function stubEl() {
    return {
        value: '', min: '', max: '', checked: false, hidden: false, open: false,
        textContent: '', innerHTML: '', className: '', href: '', download: '',
        dataset: {}, style: { setProperty() {} },
        classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
        setAttribute() {}, getAttribute: () => null,
        addEventListener() {}, removeEventListener() {},
        querySelector: () => null, querySelectorAll: () => [], closest: () => null,
        appendChild() {}, remove() {}, click() {}, focus() {},
    };
}

function stubDocument() {
    const byId = new Map();
    const bySel = new Map();
    const memo = (map, k) => { if (!map.has(k)) map.set(k, stubEl()); return map.get(k); };
    return {
        title: '',
        documentElement: stubEl(),
        body: stubEl(),
        getElementById: (id) => memo(byId, id),
        querySelector: (sel) => memo(bySel, sel),
        querySelectorAll: () => [],
        createElement: () => stubEl(),
        addEventListener() {},
    };
}

function stubStorage() {
    const map = new Map();
    return {
        get length() { return map.size; },
        key: (i) => [...map.keys()][i] ?? null,
        getItem: (k) => (map.has(k) ? map.get(k) : null),
        setItem: (k, v) => { map.set(k, String(v)); },
        removeItem: (k) => { map.delete(k); },
    };
}

const EXPORTS = `
;globalThis.__engine = {
    simulate, xirr, validatePlan, todayStr, calcDuration, userMessageFor,
    fmtUSD, fmtUnits, fmtMoney, fmtSigned, fmtPct, fmtAmount, fmtCompact, fmtDate,
    isoToDay, dayToIso, ASSETS, asset: asset(),
    heatmapGrid, heatmapJob, heatmapGeometry, heatmapCell, heatmapRowStats,
    addMonthsIso, periodLabel, lastCloseIso,
    HEAT_PERIODS, HEAT_CLAMP, HEAT_AMOUNT,
    renderHeatRows, renderHeatLegend, heatColorFor, heatColors, heatMarkerIndex, fmtRate,
    doc: document,   // the stub, so DOM-layer output can be asserted
};
`;

/** Evaluate the engine and return its pure functions. */
export function loadEngine() {
    const sandbox = {
        document: stubDocument(),
        window: { matchMedia: () => ({ matches: false }) },
        localStorage: stubStorage(),
        location: { search: '', pathname: '/', href: 'https://example.test/' },
        history: { replaceState() {} },
        navigator: { clipboard: { writeText: async () => {} } },
        // Real token values: the heatmap's colour ramp is built from them.
        getComputedStyle: () => ({ getPropertyValue: (n) => ({
            '--down': '#ff6b6b', '--live': '#3ddc84', '--accent': '#ff9a44',
            '--bg-raised': '#10142e', '--card': '#12163a', '--faint': '#7b81aa',
            '--muted': '#9ba1c5', '--text': '#e8eaf6', '--grid': 'rgba(255, 255, 255, 0.07)',
            '--series-blue': '#5b8def', '--series-neutral': '#c6c9de',
            '--live-wash': 'rgba(61, 220, 132, 0.12)', '--down-wash': 'rgba(255, 107, 107, 0.12)',
        }[n] ?? '') }),
        fetch: async () => { throw new Error('the engine tests never fetch'); },
        requestAnimationFrame: () => 0,
        cancelAnimationFrame: () => {},
        setTimeout, clearTimeout, setInterval, clearInterval,
        performance,    // the heatmap's chunked pass times itself against it
        console: { ...console, warn() {}, error() {} },
        Chart: class { static defaults = { font: {} }; update() {} destroy() {} },
        URL, Blob, DOMException,
    };
    const ctx = vm.createContext(sandbox);
    vm.runInContext(engineSource() + EXPORTS, ctx, { filename: 'index.html#inline' });
    return ctx.__engine;
}

/** SPY's ASSETS entry, the only argument simulate() needs beyond the plan. */
export function spyAsset(engine) {
    return engine.ASSETS.spy;
}

/** UTC midnight for a 'YYYY-MM-DD' string — identical in every timezone. */
export const utc = (iso) => new Date(`${iso}T00:00:00Z`);

/** The fixture close that a purchase on `iso` must fill at: the latest entry at
 *  or before that date, found by a plain scan rather than the engine's binary
 *  search, so the two are genuinely independent. */
export function closeAtOrBefore(iso) {
    const ts = Math.floor(utc(iso).getTime() / 1000);
    let found = null;
    for (const p of FIXTURE.prices) {
        if (p.ts <= ts) found = p; else break;
    }
    return found;
}
