// Engine tests: the pure core of index.html, run against tests/fixtures/prices.json.
//
// Every expectation below is pinned to the fixture and to an explicit endDate, so
// nothing here moves when the weekday price job commits new data or when the clock
// rolls over midnight.

import test from 'node:test';
import assert from 'node:assert/strict';

import { loadEngine, spyAsset, FIXTURE, utc, closeAtOrBefore } from './harness.mjs';

const engine = loadEngine();
const SPY    = spyAsset(engine);

/** Pinned "today": five days past the fixture's last close, so the final value
 *  is priced at a close that no purchase landed on. */
const END = utc('2020-11-20');

const sim = (plan) => engine.simulate(FIXTURE.prices, plan, SPY, END);

// ─────────────────────────────────────────────────────────────────────────────
// The fixture itself
// ─────────────────────────────────────────────────────────────────────────────
test('fixture is a well-formed price series with a gap in it', () => {
    const { prices } = FIXTURE;
    assert.equal(prices.length, FIXTURE.count);
    assert.equal(prices.length, 314);
    assert.equal(new Date(prices[0].ts * 1000).toISOString(), '2020-01-01T00:00:00.000Z');
    assert.equal(new Date(prices.at(-1).ts * 1000).toISOString(), '2020-11-15T00:00:00.000Z');

    let gaps = 0, flats = 0, ups = 0, downs = 0;
    for (let i = 1; i < prices.length; i++) {
        assert.ok(prices[i].ts > prices[i - 1].ts, `ts must ascend at ${i}`);
        assert.ok(prices[i].price > 0, `price must be positive at ${i}`);
        const step = (prices[i].ts - prices[i - 1].ts) / 86400;
        if (step > 1) gaps++;
        if (prices[i].price === prices[i - 1].price) flats++;
        else if (prices[i].price > prices[i - 1].price) ups++;
        else downs++;
    }
    assert.equal(gaps, 1, 'exactly one multi-day gap');
    assert.ok(flats > 30, 'a flat stretch');
    assert.ok(ups > 100 && downs > 30, 'rises and falls');
});

// ─────────────────────────────────────────────────────────────────────────────
// Golden results
// ─────────────────────────────────────────────────────────────────────────────
// `annual` is deliberately absent: XIRR goes through Math.pow, whose last bit is
// implementation-defined. It is covered analytically, with a tolerance, below.
//
// Every purchase costs `own + employer`, so a plan with a match buys more shares
// per date; the last case is there to hold that split in place.
const GOLDEN = [
    {
        name:  'weekly $100 from the first close',
        plan:  { start: '2020-01-01', freq: 7, own: 100, employer: 0 },
        n:     47,
        totalInvested: 4700,
        totalOwn:      4700,
        totalEmployer: 0,
        totalUnits:    34.26628567941252,
        finalPrice:    279.1,
        finalValue:    9563.720333124034,
        profit:        4863.720333124034,
        roi:           '103.48',
        avgCost:       137.161058072419,
        meanPaid:      162.14574468085107,
        duration:      '10 months',
        finalDate:     '2020-11-15',
        labels:        47,   // last purchase is already past the final close
        last: {
            n: 47, date: '2020-11-18', price: 279.1,
            unitsBought: 0.35829451809387314, totalUnits: 34.26628567941252,
            totalInvested: 4700, portfolioValue: 9563.720333124034, profit: 4863.720333124034,
        },
    },
    {
        name:  'every 30 days, $250, mid-February start',
        plan:  { start: '2020-02-15', freq: 30, own: 250, employer: 0 },
        n:     10,
        totalInvested: 2500,
        totalOwn:      2500,
        totalEmployer: 0,
        totalUnits:    17.939299841165973,
        finalPrice:    279.1,
        finalValue:    5006.8585856694235,
        profit:        2506.8585856694235,
        roi:           '100.27',
        avgCost:       139.35883909265834,
        meanPaid:      167.55,
        duration:      '9 months',
        finalDate:     '2020-11-15',
        labels:        11,   // final close is after the last purchase: series extended
        last: {
            n: 10, date: '2020-11-11', price: 273,
            unitsBought: 0.9157509157509157, totalUnits: 17.939299841165973,
            totalInvested: 2500, portfolioValue: 4897.42885663831, profit: 2397.42885663831,
        },
    },
    {
        name:  'fortnightly $50 with a purchase inside the data gap',
        plan:  { start: '2020-05-08', freq: 14, own: 50, employer: 0 },
        n:     15,
        totalInvested: 750,
        totalOwn:      750,
        totalEmployer: 0,
        totalUnits:    5.1576915298442945,
        finalPrice:    279.1,
        finalValue:    1439.5117059795427,
        profit:        689.5117059795427,
        roi:           '91.93',
        avgCost:       145.41389217641748,
        meanPaid:      173.9,
        duration:      '6 months',
        finalDate:     '2020-11-15',
        labels:        15,
        last: {
            n: 15, date: '2020-11-20', price: 279.1,
            unitsBought: 0.17914725904693657, totalUnits: 5.1576915298442945,
            totalInvested: 750, portfolioValue: 1439.5117059795427, profit: 689.5117059795427,
        },
    },
    {
        name:  'start before the data begins — early purchases are dropped',
        plan:  { start: '2019-06-01', freq: 30, own: 500, employer: 0 },
        n:     10,
        totalInvested: 5000,
        totalOwn:      5000,
        totalEmployer: 0,
        totalUnits:    36.30384736022823,
        finalPrice:    279.1,
        finalValue:    10132.4037982397,
        profit:        5132.4037982397,
        roi:           '102.65',
        avgCost:       137.72644949685483,
        meanPaid:      161.365,
        duration:      '1 year and 5 months',
        finalDate:     '2020-11-15',
        labels:        11,
        last: {
            n: 10, date: '2020-10-23', price: 253.65,
            unitsBought: 1.9712201852946973, totalUnits: 36.30384736022823,
            totalInvested: 5000, portfolioValue: 9208.47088292189, profit: 4208.470882921891,
        },
    },
    {
        name:  '401(k)-style: bi-weekly $200 with a $100 employer match',
        plan:  { start: '2020-02-14', freq: 14, own: 200, employer: 100 },
        n:     21,
        totalInvested: 6300,   // 21 × ($200 + $100): the match is money invested
        totalOwn:      4200,
        totalEmployer: 2100,
        totalUnits:    45.16150103351838,
        finalPrice:    279.1,
        finalValue:    12604.57493845498,
        profit:        6304.57493845498,
        roi:           '100.07',
        avgCost:       139.4993491320009,
        meanPaid:      168.73809523809524,
        duration:      '9 months',
        finalDate:     '2020-11-15',
        labels:        21,
        last: {
            n: 21, date: '2020-11-20', price: 279.1,
            unitsBought: 1.0748835542816193, totalUnits: 45.16150103351838,
            totalInvested: 6300, portfolioValue: 12604.57493845498, profit: 6304.57493845498,
        },
    },
];

for (const g of GOLDEN) {
    test(`golden: ${g.name}`, () => {
        const r = sim(g.plan);
        const perBuy = g.plan.own + g.plan.employer;
        assert.equal(r.purchases.length, g.n);
        assert.equal(r.totalInvested, g.totalInvested);
        assert.equal(r.totalInvested, g.n * perBuy, 'every purchase is the same size');
        assert.equal(r.totalOwn,      g.totalOwn);
        assert.equal(r.totalEmployer, g.totalEmployer);
        assert.equal(r.employerAmount, g.plan.employer);
        assert.equal(r.totalOwn + r.totalEmployer, r.totalInvested, 'the split adds up');
        assert.equal(r.totalUnits,  g.totalUnits);
        assert.equal(r.finalPrice,  g.finalPrice);
        assert.equal(r.finalValue,  g.finalValue);
        assert.equal(r.profit,      g.profit);
        assert.equal(r.roi,         g.roi);
        assert.equal(r.avgCost,     g.avgCost);
        assert.equal(r.meanPaid,    g.meanPaid);
        assert.equal(r.duration,    g.duration);
        assert.equal(r.finalDate,   g.finalDate);
        assert.equal(r.labels.length, g.labels);
        assert.equal(r.investedData.length, g.labels);
        assert.equal(r.valueData.length,    g.labels);
        // Spread across the realm boundary: the row is built inside the vm.
        assert.deepEqual({ ...r.purchases.at(-1) }, g.last);
    });
}

test('golden: the fixture is priced at its own last close, not at "today"', () => {
    const r = sim(GOLDEN[0].plan);
    assert.equal(r.finalPrice, FIXTURE.prices.at(-1).price);
    // END is five days past the last close; the engine must not invent one.
    assert.equal(r.finalDate, '2020-11-15');
});

test('the employer match buys shares, it does not sit in cash', () => {
    const own   = { start: '2020-02-14', freq: 14, own: 200, employer: 0 };
    const match = { start: '2020-02-14', freq: 14, own: 200, employer: 100 };
    const a = sim(own), b = sim(match);
    assert.equal(a.purchases.length, b.purchases.length);
    // Same dates, same prices, 1.5× the dollars — so 1.5× the shares and the
    // same average cost. Only the size of the position changes.
    assert.equal(b.totalInvested, a.totalInvested * 1.5);
    assert.ok(Math.abs(b.totalUnits - a.totalUnits * 1.5) < 1e-9);
    assert.ok(Math.abs(b.avgCost - a.avgCost) < 1e-9);
    assert.equal(b.meanPaid, a.meanPaid);
    assert.equal(b.roi, a.roi);
});

// ─────────────────────────────────────────────────────────────────────────────
// XIRR against analytic cases
// ─────────────────────────────────────────────────────────────────────────────
// The solver measures time in 365.25-day years, so the closed form for a single
// contribution and a single payout is  (ratio ^ (365.25 / days) - 1) * 100.
const YEAR_DAYS = 365.25;
const analytic = (ratio, from, to) =>
    (Math.pow(ratio, YEAR_DAYS / ((utc(to) - utc(from)) / 86_400_000)) - 1) * 100;

const XIRR_CASES = [
    ['+10% over one year',   1.1, '2020-01-01', '2021-01-01', -1000, 1100],
    ['doubling over two years', 2, '2020-01-01', '2022-01-01', -1000, 2000],
    ['a 60% loss over one year', 0.4, '2020-01-01', '2021-01-01', -1000, 400],
    ['break-even',             1, '2020-01-01', '2021-01-01', -1000, 1000],
];

for (const [name, ratio, from, to, out, back] of XIRR_CASES) {
    test(`xirr: ${name}`, () => {
        const got = engine.xirr([
            { when: utc(from), amount: out },
            { when: utc(to),   amount: back },
        ]);
        const want = analytic(ratio, from, to);
        assert.ok(Number.isFinite(got), 'must converge');
        assert.ok(Math.abs(got - want) < 1e-6, `${got} ≈ ${want}`);
    });
}

test('xirr returns null when it cannot solve', () => {
    // Money only ever leaves: NPV has no sign change, so there is no rate.
    assert.equal(engine.xirr([
        { when: utc('2020-01-01'), amount: -1000 },
        { when: utc('2021-01-01'), amount: -500 },
    ]), null);
    // The mirror image, money only ever arriving.
    assert.equal(engine.xirr([
        { when: utc('2020-01-01'), amount: 1000 },
        { when: utc('2021-01-01'), amount: 500 },
    ]), null);
    // Fewer than two flows is not a rate problem at all.
    assert.equal(engine.xirr([{ when: utc('2020-01-01'), amount: -1000 }]), null);
    assert.equal(engine.xirr([]), null);
});

test('xirr on a real run is a plausible money-weighted rate', () => {
    const r = sim(GOLDEN[0].plan);
    assert.ok(Number.isFinite(r.annual), 'the fixture run converges');
    // Money-weighted beats the naive shortcut, which credits every dollar with
    // the whole elapsed period.
    const naive = (Math.pow(r.finalValue / r.totalInvested, 1 / r.spanYears) - 1) * 100;
    assert.ok(r.annual > naive, `${r.annual} > ${naive}`);
});

// ─────────────────────────────────────────────────────────────────────────────
// Documented invariants
// ─────────────────────────────────────────────────────────────────────────────
test('average cost never exceeds the mean price paid', () => {
    // Equal dollars per buy make the average cost the harmonic mean of the prices
    // paid, which can never land above their plain mean. This is the mechanic the
    // page's "averaging effect" card is built on.
    for (const g of GOLDEN) {
        const r = sim(g.plan);
        assert.ok(r.avgCost <= r.meanPaid + 1e-9, `${g.name}: ${r.avgCost} <= ${r.meanPaid}`);
        assert.equal(r.avgCost, r.totalInvested / r.totalUnits);
    }
    // And it is strict wherever the prices actually varied.
    const varied = sim(GOLDEN[0].plan);
    assert.ok(varied.avgCost < varied.meanPaid);

    // A flat stretch is the equality case: every purchase at one price. The end
    // date is pinned inside that stretch so the window cannot run past it.
    const flat = engine.simulate(
        FIXTURE.prices, { start: '2020-04-10', freq: 7, own: 100, employer: 0 }, SPY, utc('2020-05-15'));
    const flatPrices = new Set(flat.purchases.map(p => p.price));
    assert.equal(flatPrices.size, 1, 'this window is inside the flat stretch');
    assert.ok(Math.abs(flat.avgCost - flat.meanPaid) < 1e-9);
});

test('a purchase never fills at a close later than its own date', () => {
    for (const g of GOLDEN) {
        const r = sim(g.plan);
        const perBuy = g.plan.own + g.plan.employer;
        for (const p of r.purchases) {
            // Independently of the engine's binary search: the latest fixture
            // entry at or before the purchase date, found by a plain scan.
            const want = closeAtOrBefore(p.date);
            assert.ok(want, `${p.date} must have a close at or before it`);
            assert.ok(want.ts * 1000 <= utc(p.date).getTime(), 'no look-ahead');
            assert.equal(p.price, want.price, `${g.name}: ${p.date} filled at the wrong close`);
            assert.equal(p.unitsBought, perBuy / want.price);
        }
        // Purchase dates are strictly increasing and exactly `freq` days apart.
        for (let i = 1; i < r.purchases.length; i++) {
            const gapDays = (utc(r.purchases[i].date) - utc(r.purchases[i - 1].date)) / 86_400_000;
            assert.equal(gapDays % g.plan.freq, 0);
        }
    }
});

test('running totals accumulate exactly', () => {
    const g = GOLDEN.at(-1);   // the employer-match plan
    const r = sim(g.plan);
    const perBuy = g.plan.own + g.plan.employer;
    let units = 0, invested = 0;
    for (const p of r.purchases) {
        units    += p.unitsBought;
        invested += perBuy;
        assert.equal(p.totalUnits, units);
        assert.equal(p.totalInvested, invested);
        assert.equal(p.portfolioValue, p.price * p.totalUnits);
        assert.equal(p.profit, p.portfolioValue - p.totalInvested);
    }
    assert.equal(r.totalUnits, units);
});

test('a plan that lands entirely outside the data is refused', () => {
    assert.throws(
        () => engine.simulate(FIXTURE.prices, { start: '1994-01-01', freq: 7, own: 100, employer: 0 }, SPY, utc('1995-01-01')),
        (err) => /No purchases landed/.test(err.user),
    );
});

// ─────────────────────────────────────────────────────────────────────────────
// validatePlan — the start-date rule and its message must agree
// ─────────────────────────────────────────────────────────────────────────────
test('today is a legitimate start date; tomorrow is not', () => {
    const today    = engine.todayStr();
    const tomorrow = new Date(Date.parse(`${today}T00:00:00Z`) + 86_400_000).toISOString().split('T')[0];

    assert.doesNotThrow(() => engine.validatePlan({ start: today, freq: 7, own: 100, employer: 0 }, SPY));
    assert.throws(
        () => engine.validatePlan({ start: tomorrow, freq: 7, own: 100, employer: 0 }, SPY),
        (err) => err.user === "Start date can't be in the future.",
    );
});

test('validatePlan keeps its other rules, in order', () => {
    assert.throws(() => engine.validatePlan({ start: 'not-a-date', freq: 7, own: 100, employer: 0 }, SPY),
        (err) => /valid start date/.test(err.user));
    assert.throws(() => engine.validatePlan({ start: '1990-01-01', freq: 7, own: 100, employer: 0 }, SPY),
        (err) => err.user.startsWith('Pick a start date on or after'));
    assert.throws(() => engine.validatePlan({ start: '2020-01-01', freq: 7, own: NaN, employer: 0 }, SPY),
        (err) => /\$1 or more/.test(err.user));
    assert.throws(() => engine.validatePlan({ start: '2020-01-01', freq: 7, own: 100, employer: -1 }, SPY),
        (err) => /match cannot be negative/.test(err.user));
    // The floor is checked before the future check: a date that fails both says so.
    assert.throws(() => engine.validatePlan({ start: '1990-01-01', freq: 7, own: 100, employer: 0 }, SPY),
        (err) => !/future/.test(err.user));
});

// ─────────────────────────────────────────────────────────────────────────────
// Formatters
// ─────────────────────────────────────────────────────────────────────────────
test('formatters', () => {
    assert.equal(engine.fmtUSD(1234.5), '$1,234.50');
    assert.equal(engine.fmtUSD(-1234.56), '-$1,234.56');   // sign outside the symbol
    assert.equal(engine.fmtUnits(1.234567891234), '1.234568');    // shares are 6dp
    assert.equal(engine.fmtMoney(999.5), '$999.50');       // cents below $1,000
    assert.equal(engine.fmtMoney(1000.4), '$1,000');       // whole dollars above
    assert.equal(engine.fmtSigned(-12.49), '-$12.49');
    assert.equal(engine.fmtSigned(4458187.2), '+$4,458,187');
    assert.equal(engine.fmtPct(-3.5), '-3.50%');
    assert.equal(engine.fmtPct(6377.9512), '+6,377.95%');
    assert.equal(engine.fmtCompact(4500000), '$4.5M');
    assert.equal(engine.fmtAmount(100), '$100');
    assert.equal(engine.fmtAmount(37.5), '$37.50');
    assert.equal(engine.fmtDate('1993-01-29'), 'Jan 29, 1993');  // UTC, never shifted
    assert.equal(engine.fmtDate('2020-01-01'), 'Jan 1, 2020');
});

test('calcDuration reads in whole years and months', () => {
    assert.equal(engine.calcDuration(utc('2020-01-01'), utc('2020-01-15')), 'less than a month');
    assert.equal(engine.calcDuration(utc('2020-01-01'), utc('2020-04-01')), '2 months');
    assert.equal(engine.calcDuration(utc('2020-01-01'), utc('2022-07-01')), '2 years and 5 months');
});

// ─────────────────────────────────────────────────────────────────────────────
// End-date handling (both branches of the second binary search)
// ─────────────────────────────────────────────────────────────────────────────
test('an end date inside the series prices the stack at that close, not the newest row', () => {
    const r = engine.simulate(FIXTURE.prices, { start: '2020-01-01', freq: 7, own: 100, employer: 0 }, SPY, utc('2020-06-15'));
    assert.equal(r.purchases.length, 24);
    assert.equal(r.finalDate, '2020-06-15');
    assert.equal(r.finalPrice, 102.65);
    assert.equal(r.finalValue, r.totalUnits * 102.65);
    assert.notEqual(r.finalPrice, FIXTURE.prices.at(-1).price, 'must not fall back to the last row');
});

test('a purchase landing on the final close adds no extra chart point', () => {
    const r = engine.simulate(FIXTURE.prices, { start: '2020-11-15', freq: 7, own: 100, employer: 0 }, SPY, END);
    assert.equal(r.purchases.length, 1);
    assert.equal(r.purchases[0].date, '2020-11-15');
    assert.equal(r.finalDate, '2020-11-15');
    assert.equal(r.labels.length, 1, 'equality must not append a duplicate point');
    assert.equal(r.labels.at(-1), r.finalDate);
    assert.equal(r.investedData.length, 1);
    assert.equal(r.valueData.length, 1);
});
