// Timezone independence.
//
// simulate() steps its schedule with setUTCDate() on a date parsed from
// 'YYYY-MM-DD' — UTC midnight. The local-time equivalent, setDate(), preserves
// the wall-clock hour across a daylight-saving change, which shifts every later
// purchase by an hour; in a timezone behind UTC that rolls a purchase back onto
// the previous UTC day, where it fills at the previous close.
//
// This runs the same plans in child processes under three timezones — one with
// no DST, one behind UTC that changes in March and November, one ahead of UTC
// that changes in April and October — and requires identical output. Reverting
// setUTCDate() to setDate() fails it.

import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE     = path.dirname(fileURLToPath(import.meta.url));
const RUN_PLAN = path.join(HERE, 'run-plan.mjs');

const ZONES = ['UTC', 'America/New_York', 'Australia/Sydney'];

const runIn = (tz) => JSON.parse(execFileSync(process.execPath, [RUN_PLAN], {
    env: { ...process.env, TZ: tz },
    encoding: 'utf8',
}));

test('the same plan gives the same result in every timezone', () => {
    const runs = ZONES.map((tz) => [tz, runIn(tz)]);

    // The timezone really did take hold — otherwise this test would pass by
    // running three identical processes.
    const offsets = runs.map(([, out]) => `${out.env.winter}/${out.env.summer}`);
    assert.equal(new Set(offsets).size, ZONES.length,
        `each zone must have its own UTC offsets, got ${offsets.join(' ')}`);
    for (const [tz, out] of runs) {
        if (tz === 'UTC') assert.equal(out.env.winter, out.env.summer, 'UTC has no DST');
        else assert.notEqual(out.env.winter, out.env.summer, `${tz} must observe DST`);
    }

    // Every figure the engine produces is identical.
    const [[baseTz, base], ...rest] = runs;
    const canonical = JSON.stringify(base.results);
    for (const [tz, out] of rest) {
        assert.equal(JSON.stringify(out.results), canonical,
            `${tz} disagrees with ${baseTz} — the schedule is stepping in local time`);
    }
});

test('purchase dates stay on their UTC cadence across a DST change', () => {
    // 2020-03-07 + 7 days must be 2020-03-14, not 2020-03-13, in a zone whose
    // clocks moved on 2020-03-08.
    for (const tz of ZONES) {
        const { results } = runIn(tz);
        const weekly = results.find(r => r.plan.start === '2020-03-07');
        const dates  = weekly.purchases.map(p => p.split('@')[0]);
        assert.equal(dates[0], '2020-03-07');
        assert.equal(dates[1], '2020-03-14', `${tz}: the step after the DST change drifted`);
        for (let i = 1; i < dates.length; i++) {
            const days = (Date.parse(`${dates[i]}T00:00:00Z`) - Date.parse(`${dates[i - 1]}T00:00:00Z`)) / 86_400_000;
            assert.equal(days, 7, `${tz}: ${dates[i - 1]} → ${dates[i]} is not 7 days`);
        }
    }
});
