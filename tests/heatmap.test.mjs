// Start-date heatmap tests: the pure grid builder from index.html, run against
// tests/fixtures/prices.json.
//
// The fixture spans 2020-01-01 → 2020-11-15, which is less than a year, so the
// production row set (1, 2, 3, 5 and 10 years) cannot fill a single window on it.
// That is itself worth asserting — and it is why the builder takes its holding
// periods in MONTHS and as a parameter: the same code path can then be driven at
// month scale here without the tests needing a decade of synthetic prices.
//
// The fixture's quarterly `dividends` array is passed to every grid, so the
// reinvestment switch — the second half of this page's grid identity — is
// exercised on both settings rather than only on the page's default.

import test from 'node:test';
import assert from 'node:assert/strict';

import { loadEngine, spyAsset, FIXTURE, utc } from './harness.mjs';

const engine = loadEngine();
const SPY    = spyAsset(engine);

/** Pinned "today": five days past the fixture's last close, exactly as the
 *  engine tests pin it, so nothing here moves with the clock. */
const END = utc('2020-11-20');

/** The last close at or before END — every window has to finish by this date. */
const END_ISO = '2020-11-15';

/** Month-scale stand-ins for the production rows. */
const MONTHS = [1, 2, 3];

const grid = (freq, periods = MONTHS, a = SPY, reinvest = false) =>
    engine.heatmapGrid(FIXTURE.prices, a, freq, reinvest, FIXTURE.dividends, END, periods);

/** The reference a cell has to match: a plain simulate() on the same plan, with
 *  no seeded guess and nothing else shared with the grid builder. */
function reference(startIso, months, freq, own = 100, employer = 0, reinvest = false) {
    const end  = utc(engine.addMonthsIso(startIso, months));
    const plan = { start: startIso, freq, own, employer, reinvest };
    return engine.simulate(FIXTURE.prices, plan, SPY, end, FIXTURE.dividends).annual;
}

// XIRR is Newton–Raphson against an ABSOLUTE residual, so two runs of the same
// window can stop a hair apart: seeding the solve with a neighbour's answer (what
// the grid does) or scaling the flows (what a different contribution amount, or
// an employer match, does) both move where that residual falls below the
// threshold. What is invariant is the rate itself, to within the solver's
// precision — so these compare relatively. Six significant figures is ~0.002
// percentage points on the page's largest real figure, three orders below the
// 0.1pp it ever displays.
const TOL = 1e-6;

/** `got` and `want` agree to six significant figures. */
function close(got, want, what) {
    assert.ok(Math.abs(got - want) <= TOL * Math.max(1, Math.abs(want)),
        `${what}: ${got} vs ${want}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Date arithmetic the geometry rests on
// ─────────────────────────────────────────────────────────────────────────────
test('addMonthsIso steps whole months in UTC, clamping to the month end', () => {
    assert.equal(engine.addMonthsIso('2020-01-01', 1),   '2020-02-01');
    assert.equal(engine.addMonthsIso('2020-01-31', 1),   '2020-02-29');  // leap year, clamped
    assert.equal(engine.addMonthsIso('2021-01-31', 1),   '2021-02-28');
    assert.equal(engine.addMonthsIso('2020-03-31', 1),   '2020-04-30');
    assert.equal(engine.addMonthsIso('2020-11-30', 2),   '2021-01-30');  // year rollover
    assert.equal(engine.addMonthsIso('1993-01-29', 12),  '1994-01-29');  // SPY's first trading day
    assert.equal(engine.addMonthsIso('1993-01-29', 120), '2003-01-29');
    assert.equal(engine.addMonthsIso('2020-02-29', 12),  '2021-02-28');  // leap day, clamped
    assert.equal(engine.addMonthsIso('2020-06-15', 0),   '2020-06-15');
});

test('periodLabel reads in years once a period divides into them', () => {
    assert.equal(engine.periodLabel(12),  '1 year');
    assert.equal(engine.periodLabel(24),  '2 years');
    assert.equal(engine.periodLabel(120), '10 years');
    assert.equal(engine.periodLabel(1),   '1 month');
    assert.equal(engine.periodLabel(6),   '6 months');
});

test('lastCloseIso is the newest close at or before the end date, not the end date', () => {
    assert.equal(engine.lastCloseIso(FIXTURE.prices, END), END_ISO);
    assert.equal(engine.lastCloseIso(FIXTURE.prices, utc('2020-06-10')), '2020-06-10');
    // ...and it walks back over the fixture's one multi-day gap rather than past it
    const inGap = engine.lastCloseIso(FIXTURE.prices, utc('2020-07-04'));
    assert.ok(inGap <= '2020-07-04');
    assert.ok(FIXTURE.prices.some(p => new Date(p.ts * 1000).toISOString().split('T')[0] === inGap));
});

// ─────────────────────────────────────────────────────────────────────────────
// A cell is simulate(), not a second implementation of it
// ─────────────────────────────────────────────────────────────────────────────
test('every cell equals simulate()\'s own annual for the same plan', () => {
    for (const reinvest of [false, true]) {
        for (const freq of [7, 14, 30]) {
            const g = grid(freq, MONTHS, SPY, reinvest);
            let checked = 0;
            for (const row of g.rows) {
                assert.equal(row.cells.length, row.count, `${row.label}: one cell per reachable column`);
                row.cells.forEach((v, j) => {
                    const want  = reference(g.columns[j].start, row.months, freq, 100, 0, reinvest);
                    const where = `${row.label} @ ${g.columns[j].start} (reinvest ${reinvest})`;
                    // The fixture's March 2020 crash has no 1-month rate that solves.
                    // The grid must report that the same way simulate() does, not
                    // paper over it — so a null on one side has to be a null on both.
                    assert.equal(v === null, want === null, `${where}: both solve, or neither`);
                    if (v !== null) close(v, want, where);
                    checked++;
                });
            }
            assert.ok(checked > 20, `freq ${freq}: a meaningful number of cells`);
            assert.equal(checked, g.cellCount);
            const holes = g.rows.flatMap(r => r.cells).filter(v => v === null).length;
            assert.equal(holes, 1, `freq ${freq}: exactly the one unsolvable window`);
        }
    }
});

test('the chunked pass and the one-shot pass agree cell for cell', () => {
    // Reinvestment on, because that is the page's default and the slower path.
    const job = engine.heatmapJob(FIXTURE.prices, SPY, 7, true, FIXTURE.dividends, END, MONTHS);
    let steps = 0;
    // A budget of 0 forces a yield after every single cell — the harshest
    // possible chunking, and it must not change a thing.
    while (!job.step(0)) { steps++; assert.ok(steps < 5000, 'the pass terminates'); }
    assert.ok(steps > 10, 'the pass really was interrupted');
    assert.equal(job.computed, job.grid.cellCount);
    assert.deepEqual(job.grid.rows.map(r => r.cells), grid(7, MONTHS, SPY, true).rows.map(r => r.cells));
});

// ─────────────────────────────────────────────────────────────────────────────
// Amount- and match-invariance — the claim the panel copy makes
// ─────────────────────────────────────────────────────────────────────────────
test('a cell does not depend on the contribution amount or the employer match', () => {
    // Every pair puts different dollars in on exactly the same dates: a bigger
    // contribution, a match on top of it, or the same total split differently
    // between the two. None of it may move the rate.
    const PAIRS = [[100, 0], [250, 0], [1, 0], [5000, 0], [100, 50], [100, 400], [37.5, 12.5]];
    for (const reinvest of [false, true]) {
        const g = grid(7, MONTHS, SPY, reinvest);
        for (const row of g.rows) {
            row.cells.forEach((v, j) => {
                if (v === null) return;
                for (const [own, employer] of PAIRS) {
                    close(v, reference(g.columns[j].start, row.months, 7, own, employer, reinvest),
                        `${row.label} @ ${g.columns[j].start} at $${own} + $${employer} match`);
                }
            });
        }
    }
});

test('scaling every flow leaves the rate exactly where it was', () => {
    // The general statement behind the panel's "changing your contribution or
    // your employer match moves nothing here": a match is just a larger flow on
    // the same dates, and scaling the flows scales the NPV by the same factor,
    // so its root cannot move.
    const flows = [
        { when: utc('2020-01-01'), amount: -100 },
        { when: utc('2020-04-01'), amount: -100 },
        { when: utc('2020-07-01'), amount: -100 },
        { when: utc('2021-01-01'), amount:  340 },
    ];
    const base = engine.xirr(flows);
    assert.ok(base !== null);
    for (const k of [2.5, 1.5, 0.01, 1000]) {
        const scaled = engine.xirr(flows.map(f => ({ when: f.when, amount: f.amount * k })));
        close(scaled, base, `scaled by ${k}`);
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Row and column geometry
// ─────────────────────────────────────────────────────────────────────────────
test('the production rows are dropped whole when the data cannot fill them', () => {
    assert.deepEqual([...engine.HEAT_PERIODS], [12, 24, 36, 60, 120]);
    // The fixture is 10.5 months long: not one of 1, 2, 3, 5 or 10 years fits.
    const g = engine.heatmapGrid(FIXTURE.prices, SPY, 7, true, FIXTURE.dividends, END, engine.HEAT_PERIODS);
    assert.deepEqual([...g.rows], []);
    assert.deepEqual([...g.columns], []);
    assert.equal(g.cellCount, 0);
});

test('a row survives only if its first window fits, and reaches only as far as it can', () => {
    const g = grid(30, [1, 2, 3, 6, 12]);

    assert.deepEqual([...g.rows.map(r => r.months)], [1, 2, 3, 6], '12 months cannot be filled');
    // Columns run from the floor month to the last month the SHORTEST row can fill
    assert.equal(g.columns.length, 10);
    assert.equal(g.columns[0].start, '2020-01-01');
    assert.equal(g.columns.at(-1).start, '2020-10-01');
    assert.deepEqual([...g.rows.map(r => r.count)], [10, 9, 8, 5]);
    assert.equal(g.cellCount, 32);
    assert.equal(g.endIso, END_ISO);

    for (const row of g.rows) {
        // the last reachable column really does finish on or before the last close
        assert.ok(engine.addMonthsIso(g.columns[row.count - 1].start, row.months) <= g.endIso,
            `${row.label}: last cell ends by ${g.endIso}`);
        // and the next one really would not
        if (row.count < g.columns.length) {
            assert.ok(engine.addMonthsIso(g.columns[row.count].start, row.months) > g.endIso,
                `${row.label}: column ${row.count} would run past ${g.endIso}`);
        }
        assert.equal(row.cells.length, row.count, `${row.label}: no cell beyond its reach`);
    }
});

test('the first column starts on the date floor, later ones on the 1st', () => {
    // A floor inside the data, so it is the picker's floor that bites and not the
    // first close. (For SPY proper it is the other way round on this fixture.)
    const late = { ...SPY, minDate: '2020-01-15' };
    const g = grid(7, [1, 2], late);
    assert.equal(g.floorIso, '2020-01-15');
    assert.equal(g.columns[0].start, '2020-01-15');
    assert.equal(g.columns[0].month, '2020-01-01');
    assert.equal(g.columns[1].start, '2020-02-01');
    assert.ok(g.columns.every((c, i) => i === 0 || c.start === c.month));
    // The floor is never earlier than the data, whatever the picker says
    const early = { ...SPY, minDate: '1993-01-29' };
    assert.equal(grid(7, [1], early).floorIso, '2020-01-01');
});

test('columns are consecutive months, labelled by month and year', () => {
    const g = grid(7);
    assert.equal(g.columns[0].label, 'Jan 2020');
    assert.equal(g.columns.at(-1).label, 'Oct 2020');
    g.columns.forEach((c, i) => {
        if (i === 0) return;
        assert.equal(c.month, engine.addMonthsIso(g.columns[i - 1].month, 1), 'no month skipped');
        assert.ok(c.start > g.columns[i - 1].start, 'start months ascend');
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Row summaries — the accessible twin of the canvas
// ─────────────────────────────────────────────────────────────────────────────
test('heatmapRowStats, by hand', () => {
    // Even count: the median is the mean of the two middle values.
    const even = engine.heatmapRowStats([3, -1, 5, 2]);
    assert.equal(even.count, 4);
    assert.equal(even.worst, -1);
    assert.equal(even.median, 2.5);       // (2 + 3) / 2
    assert.equal(even.best, 5);
    assert.equal(even.positive, 3);
    assert.equal(even.positiveShare, 75);

    // Odd count: the middle value itself.
    const odd = engine.heatmapRowStats([-8, -2, -30, 4, 1]);
    assert.equal(odd.count, 5);
    assert.equal(odd.worst, -30);
    assert.equal(odd.median, -2);
    assert.equal(odd.best, 4);
    assert.equal(odd.positive, 2);
    assert.equal(odd.positiveShare, 40);

    // Cells XIRR could not solve are left out, not counted as zero.
    const holes = engine.heatmapRowStats([null, 10, null, 30, 20]);
    assert.equal(holes.count, 3);
    assert.equal(holes.median, 20);
    assert.equal(holes.positiveShare, 100);

    // `{...}` because the engine runs in its own realm: a plain object from in
    // there is not deepStrictEqual to one built out here.
    const none = { ...engine.heatmapRowStats([null, null]) };
    assert.deepEqual(none, { count: 0, worst: null, median: null, best: null, positive: 0, positiveShare: null });

    // A single cell is its own worst, median and best.
    const one = { ...engine.heatmapRowStats([-4.5]) };
    assert.deepEqual(one, { count: 1, worst: -4.5, median: -4.5, best: -4.5, positive: 0, positiveShare: 0 });
});

test('every row carries stats that match its own cells', () => {
    const g = grid(14, MONTHS, SPY, true);
    for (const row of g.rows) {
        const sorted = row.cells.filter(v => v != null).sort((x, z) => x - z);
        assert.equal(row.stats.count, sorted.length);
        assert.equal(row.stats.worst, sorted[0]);
        assert.equal(row.stats.best, sorted.at(-1));
        assert.equal(row.stats.median,
            sorted.length % 2 ? sorted[(sorted.length - 1) / 2]
                              : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2);
        assert.equal(row.stats.positive, sorted.filter(v => v > 0).length);
        assert.equal(row.stats.positiveShare, row.stats.positive / sorted.length * 100);
        assert.ok(row.stats.worst <= row.stats.median && row.stats.median <= row.stats.best);
    }
});

test('the fixture\'s own spread, worked through by hand', () => {
    // The fixture falls hard through its first quarter and then climbs, so the
    // eight 3-month windows on it split three losses to five gains. Every figure
    // below is read off those eight cells, not off the implementation.
    const g   = grid(30, [3]);
    const row = g.rows[0];
    assert.equal(row.label, '3 months');
    assert.equal(row.count, 8);

    const cells = [...row.cells.map(v => Number(v.toFixed(3)))];
    assert.deepEqual(cells, [-85.999, -99.090, -82.589, 457.706, 2107.754, 1769.015, 1009.498, 598.268]);

    // sorted: -99.090  -85.999  -82.589  457.706  598.268  1009.498  1769.015  2107.754
    //                                    ^^^^^^^  ^^^^^^^  the two middle values
    assert.equal(Number(row.stats.worst.toFixed(3)),  -99.090);
    assert.equal(Number(row.stats.best.toFixed(3)),  2107.754);
    assert.equal(Number(row.stats.median.toFixed(3)), Number(((457.706 + 598.268) / 2).toFixed(3)));
    assert.equal(row.stats.median, (row.cells[3] + row.cells[7]) / 2);
    assert.equal(row.stats.positive, 5);
    assert.equal(row.stats.positiveShare, 62.5);      // 5 of 8

    // and those eight figures are the ones simulate() gives for those months
    row.cells.forEach((v, j) => {
        close(v, reference(g.columns[j].start, 3, 30), `3 months @ ${g.columns[j].start}`);
    });
});

test('the same spread with dividends reinvested, worked through by hand', () => {
    // The page's default. Same eight windows, same split of three losses to five
    // gains — every one of them a little higher, because the fixture's quarterly
    // distributions bought more shares along the way.
    const g   = grid(30, [3], SPY, true);
    const row = g.rows[0];
    assert.equal(row.count, 8);

    const cells = [...row.cells.map(v => Number(v.toFixed(3)))];
    assert.deepEqual(cells, [-85.009, -99.064, -82.459, 502.958, 2233.720, 1831.647, 1060.384, 621.611]);

    // sorted: -99.064  -85.009  -82.459  502.958  621.611  1060.384  1831.647  2233.720
    //                                    ^^^^^^^  ^^^^^^^  the two middle values
    assert.equal(Number(row.stats.worst.toFixed(3)),  -99.064);
    assert.equal(Number(row.stats.best.toFixed(3)),  2233.720);
    assert.equal(Number(row.stats.median.toFixed(3)), Number(((502.958 + 621.611) / 2).toFixed(3)));
    assert.equal(row.stats.median, (row.cells[3] + row.cells[7]) / 2);
    assert.equal(row.stats.positive, 5);
    assert.equal(row.stats.positiveShare, 62.5);      // 5 of 8

    row.cells.forEach((v, j) => {
        close(v, reference(g.columns[j].start, 3, 30, 100, 0, true), `3 months @ ${g.columns[j].start}`);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Reinvestment — the other half of this page's grid identity
// ─────────────────────────────────────────────────────────────────────────────
test('the dividend switch moves the cells, and only the cells', () => {
    const off = grid(30);
    const on  = grid(30, MONTHS, SPY, true);

    assert.equal(off.reinvest, false);
    assert.equal(on.reinvest,  true);
    // Geometry is a property of the price series and the calendar, not of dividends
    assert.deepEqual(off.columns.map(c => c.start), on.columns.map(c => c.start));
    assert.deepEqual(off.rows.map(r => r.count),    on.rows.map(r => r.count));
    assert.notDeepEqual(off.rows.map(r => r.cells), on.rows.map(r => r.cells));

    // Reinvesting a distribution can never cost you: no cell comes out lower.
    off.rows.forEach((row, i) => row.cells.forEach((v, j) => {
        if (v === null) { assert.equal(on.rows[i].cells[j], null); return; }
        const w = on.rows[i].cells[j];
        assert.ok(w - v >= -TOL * Math.max(1, Math.abs(v)),
            `${row.label} @ ${off.columns[j].start}: ${w} < ${v} with dividends reinvested`);
    }));

    // Every 3-month window contains at least one of the fixture's ex-dates, so
    // every cell in that row is strictly higher.
    const [offQ, onQ] = [grid(30, [3]).rows[0], grid(30, [3], SPY, true).rows[0]];
    offQ.cells.forEach((v, j) => assert.ok(onQ.cells[j] > v, `3 months, column ${j}`));

    // A window with no ex-date inside it is the same cell either way. April 2020
    // sits between the fixture's March and June distributions.
    const jan = grid(30, [1]), janOn = grid(30, [1], SPY, true);
    assert.equal(jan.columns[3].start, '2020-04-01');
    close(janOn.rows[0].cells[3], jan.rows[0].cells[3], 'April 2020, one month, no ex-date inside');
});

// ─────────────────────────────────────────────────────────────────────────────
// The grid the page hands the canvas
// ─────────────────────────────────────────────────────────────────────────────
test('the grid reports the identity, the clamp and the amount it was built at', () => {
    const g = grid(14, MONTHS, SPY, true);
    assert.equal(g.freq, 14);
    assert.equal(g.reinvest, true);
    assert.equal(g.clamp, engine.HEAT_CLAMP);
    assert.equal(g.amount, engine.HEAT_AMOUNT);
    assert.equal(g.amount, 100);
    assert.ok(g.clamp > 0, 'the clamp is symmetric, so one positive number states it');
    assert.equal(g.cellCount, g.rows.reduce((s, r) => s + r.count, 0));
});

test('frequency is the other thing that moves the grid', () => {
    const weekly  = grid(7);
    const monthly = grid(30);
    assert.deepEqual(weekly.columns.map(c => c.start), monthly.columns.map(c => c.start));
    assert.deepEqual(weekly.rows.map(r => r.count),    monthly.rows.map(r => r.count));
    assert.notDeepEqual(weekly.rows.map(r => r.cells), monthly.rows.map(r => r.cells));
});

/** The frequency these DOM tests drive the grid at. */
const FREQ = 14;

// ─────────────────────────────────────────────────────────────────────────────
// The accessible twin: the row summary, the legend, the colour ramp, the marker.
// These are what a screen reader and a colour-blind visitor actually read, so
// they are asserted as text and as numbers rather than trusted to the canvas.
// ─────────────────────────────────────────────────────────────────────────────

/** Cross-realm safe: the engine runs in a vm, so its objects fail deepEqual. */
const px = (c) => c && [c.r, c.g, c.b];

/** Each rendered <li>, read the way a screen reader meets it: the label, then
 *  the three rates in the order they are announced, then the positive share. */
function readHeatRows() {
    const html = engine.doc.getElementById('heatRows').innerHTML;
    const text = (s) => s.replace(/<[^>]*>/g, '');
    return html.split('<li>').slice(1).map((li) => {
        const m = /worst(.*?)&middot;\s*median(.*?)&middot;\s*best(.*?)&middot;(.*?)of ([\d,]+) start months/s.exec(li);
        return {
            label: text(/<b>(.*?)<\/b>/s.exec(li)?.[1] ?? ''),
            raw:   li,
            ...(m && { worst: text(m[1]).trim(), median: text(m[2]).trim(), best: text(m[3]).trim(),
                       share: text(m[4]).trim(), count: m[5] }),
        };
    });
}

test('the row summary announces each row’s own worst, median, best and positive share', () => {
    const g = grid(FREQ);
    engine.renderHeatRows(g);
    const filled = g.rows.filter(r => r.stats && r.stats.count);
    const shown  = readHeatRows();
    assert.ok(filled.length, 'the fixture fills at least one row');
    assert.equal(shown.length, filled.length, 'one line per filled row, and no more');

    for (const [i, row] of filled.entries()) {
        const s = row.stats, out = shown[i];
        assert.equal(out.label, row.label, 'the row is named');
        // Positional, not merely present: worst and best must not be able to swap.
        assert.equal(out.worst,  engine.fmtRate(s.worst),  `${row.label}: worst`);
        assert.equal(out.median, engine.fmtRate(s.median), `${row.label}: median`);
        assert.equal(out.best,   engine.fmtRate(s.best),   `${row.label}: best`);
        assert.equal(out.share,  `${Math.round(s.positiveShare)}%`, `${row.label}: positive share`);
        assert.equal(out.count,  s.count.toLocaleString('en-US'), `${row.label}: start months counted`);
        assert.ok(s.worst <= s.median && s.median <= s.best, `${row.label}: the stats themselves are ordered`);
    }
});

test('a grid no row can fill says so rather than rendering an empty list', () => {
    engine.renderHeatRows(grid(FREQ, [120]));
    assert.match(engine.doc.getElementById('heatRows').innerHTML, /too short/i);
});

test('the legend names every tick it draws, at the grid’s own clamp', () => {
    const g = grid(FREQ);
    engine.renderHeatLegend(g);
    const ticks = [...engine.doc.getElementById('heatTicks').innerHTML.matchAll(/<span>(.*?)<\/span>/gs)]
        .map(m => m[1]);
    // Exact, in order: a tick that drifts off the clamp mislabels every cell.
    assert.deepEqual(ticks, [
        `≤ −${g.clamp}%/yr`, `−${g.clamp / 2}%`, '0', `+${g.clamp / 2}%`, `≥ +${g.clamp}%/yr`,
    ]);

    const ramp = engine.doc.getElementById('heatRamp').style.background;
    assert.match(ramp, /^linear-gradient\(90deg, /);
    const stops = [...ramp.matchAll(/(rgb\([^)]*\))\s+(\d+)%/g)];
    assert.equal(stops.length, 11, 'eleven stops');
    assert.deepEqual(stops.map(s => Number(s[2])), [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
});

test('the colour ramp runs loss → neutral → gain, and clamps at both ends', () => {
    const ramp = { down: { r: 255, g: 107, b: 107 }, mid: { r: 20, g: 24, b: 46 }, live: { r: 61, g: 220, b: 132 } };
    assert.deepEqual(px(engine.heatColorFor(0, 100, ramp)), px(ramp.mid), 'zero is neutral');
    assert.deepEqual(px(engine.heatColorFor(100, 100, ramp)), px(ramp.live), 'the clamp is full gain');
    assert.deepEqual(px(engine.heatColorFor(400, 100, ramp)), px(ramp.live), 'clamped above');
    assert.deepEqual(px(engine.heatColorFor(-100, 100, ramp)), px(ramp.down), 'the clamp is full loss');
    assert.deepEqual(px(engine.heatColorFor(-400, 100, ramp)), px(ramp.down), 'clamped below');
    assert.equal(engine.heatColorFor(null, 100, ramp), null, 'an unsolved cell has no colour');
    assert.equal(engine.heatColorFor(Infinity, 100, ramp), null, 'nor does a rate that ran away');
    assert.ok(engine.heatColorFor(5, 100, ramp).g > ramp.mid.g + 10, 'a +5%/yr cell still reads as a gain');
    assert.ok(engine.heatColorFor(-5, 100, ramp).r > ramp.mid.r + 10, 'and a −5%/yr cell as a loss');
});

test('the start marker maps a start date to its column, or reports none', () => {
    const g = grid(FREQ);
    const j = Math.min(2, g.columns.length - 1);
    const month = g.columns[j].month.slice(0, 7);
    assert.equal(engine.heatMarkerIndex(g, `${month}-01`), j);
    assert.equal(engine.heatMarkerIndex(g, `${month}-17`), j, 'any day inside that month');

    // The two months just outside the grid: the off-by-one a range check invites.
    const last   = g.columns[g.columns.length - 1].month.slice(0, 7);
    const first  = g.columns[0].month.slice(0, 7);
    assert.equal(engine.heatMarkerIndex(g, `${first}-01`), 0, 'the first column is inside');
    // Outside is outside, at every distance: -1 exactly, never a slack index.
    for (const n of [1, 2, 3, 12]) {
        assert.equal(engine.heatMarkerIndex(g, engine.addMonthsIso(`${last}-01`, n)), -1, `${n} month(s) past the end`);
        assert.equal(engine.heatMarkerIndex(g, engine.addMonthsIso(`${first}-01`, -n)), -1, `${n} month(s) before the start`);
    }
    // Same month, a year out: the index must count years, not months alone.
    assert.equal(engine.heatMarkerIndex(g, engine.addMonthsIso(`${month}-01`, 12)), -1, 'a year later');
    assert.equal(engine.heatMarkerIndex(g, engine.addMonthsIso(`${month}-01`, -12)), -1, 'a year earlier');
    assert.equal(engine.heatMarkerIndex(g, ''), -1, 'no plan, no marker');
    assert.equal(engine.heatMarkerIndex({ columns: [] }, '2020-03-01'), -1, 'an empty grid has none');
});
