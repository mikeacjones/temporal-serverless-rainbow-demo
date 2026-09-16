// Run the real frontend renderer against captured snapshots, in node.
//
// check-frontend.sh catches names that do not resolve; this catches code that
// resolves and then throws. That gap has been expensive: a renderer that threw
// mid-frame left three panels blank on screen while every static check passed,
// and it happened more than once.
//
// The DOM below is the smallest thing app.js will accept. It is not a browser
// and is not trying to be — it exists so that render() runs for real against
// real data, and so the rules the order columns depend on are enforced.
import fs from 'node:fs';
import path from 'node:path';

const repo = path.resolve(import.meta.dirname, '../..');
const fixturePath = process.argv[2] || path.join(repo, 'deploy/local/fixtures/snapshot.json');
const source = fs.readFileSync(path.join(repo, 'frontend/app.js'), 'utf8');

class Element {
  constructor(tag) {
    this.tagName = tag;
    this.children = [];
    this.dataset = {};
    this.props = {};
    this.style = { setProperty: (k, v) => { this.props[k] = v; } };
    this.title = '';
    this._text = '';
    this._cls = new Set();
  }
  get className() { return [...this._cls].join(' '); }
  set className(v) { this._cls = new Set(String(v).split(/\s+/).filter(Boolean)); }
  get classList() {
    const set = this._cls;
    return {
      add: (c) => set.add(c),
      remove: (c) => set.delete(c),
      contains: (c) => set.has(c),
      toggle: (c, on) => (on ? set.add(c) : set.delete(c)),
    };
  }
  get textContent() { return this._text; }
  set textContent(v) { this._text = String(v); this.children = []; }
  append(...kids) { for (const k of kids) if (k instanceof Element) this.children.push(k); }
  appendChild(k) { this.append(k); return k; }
  replaceChildren(...kids) { this.children = kids.filter((k) => k instanceof Element); }
  insertBefore(k) { this.append(k); return k; }
  remove() {}
  setAttribute(k, v) {
    // class has to reach the class set rather than sit on the element as a
    // plain property, or a selector search can never find what was created.
    if (k === 'class') this.className = v;
    else this[k] = v;
  }
  getAttribute(k) { return k === 'class' ? this.className : this[k]; }
  addEventListener() {}
  querySelectorAll(sel) { const out = []; this._find(sel, out); return out; }
  // Only class selectors are used, and they have to actually match: the
  // sparkline code builds its SVG on the first miss then reads the parts back
  // by class, so a stub that always answers null or always answers an empty
  // element breaks in opposite directions.
  querySelector(sel) { return this.querySelectorAll(sel)[0] ?? null; }
  _find(sel, out) {
    const want = String(sel).startsWith('.') ? String(sel).slice(1) : null;
    for (const kid of this.children) {
      if (want && kid._cls.has(want)) out.push(kid);
      kid._find(sel, out);
    }
  }
  getBoundingClientRect() { return { top: 0, height: 42 }; }
  get firstChild() { return this.children[0] ?? null; }
  get parentNode() { return null; }
}

// Each scenario gets its own document and its own copy of app.js, because the
// renderer carries state between frames on purpose — which order is seated,
// which has finished, which is leaving. Sharing that between scenarios makes
// one test's orders look like another test's completions.
function newEnv() {
  const ids = new Map();
  const html = new Element('html');
  const document = {
    documentElement: html,
    createElement: (t) => new Element(t),
    createElementNS: (_ns, t) => new Element(t),
    createTextNode: (t) => { const n = new Element('#text'); n.textContent = String(t); return n; },
    getElementById(id) {
      if (!ids.has(id)) ids.set(id, new Element('div'));
      return ids.get(id);
    },
    querySelectorAll: () => [],
    querySelector: () => null,
    addEventListener() {},
    body: new Element('body'),
  };

  const departures = [];
  const realSetTimeout = globalThis.setTimeout;

  globalThis.document = document;
  globalThis.window = { location: { origin: 'http://localhost', pathname: '/' }, addEventListener() {} };
  globalThis.location = globalThis.window.location;
  globalThis.EventSource = class { addEventListener() {} close() {} };
  globalThis.requestAnimationFrame = (fn) => fn();
  // The page fetches once on load; the harness drives render() itself.
  globalThis.fetch = () => Promise.reject(new Error('smoke test: no network'));
  globalThis.setTimeout = (fn, ms, ...rest) => {
    if (ms === 420 || ms === 150) departures.push(ms);
    return realSetTimeout(fn, ms, ...rest);
  };

  // render() isolates each panel, so a broken panel logs rather than throwing
  // — which is exactly how a blank panel shipped before. Watch the log.
  const failures = [];
  const realError = console.error;
  console.error = (...args) => {
    if (String(args[0] ?? '').includes('failed to draw')) failures.push(args.map(String).join(' '));
    else realError(...args);
  };

  let api;
  try {
    api = new Function(`${source}\nreturn { render, departing };`)();
  } finally {
    console.error = realError;
  }

  return {
    api,
    html,
    failures,
    departures,
    columns() {
      return new Map(document.getElementById('order-columns').children
        .map((c) => [c.children[0].children[0].textContent, c.children[1]]));
    },
    text(id) { return document.getElementById(id).textContent; },
    draw(snapshot, frames = 1) {
      for (let i = 0; i < frames; i += 1) {
        const realError2 = console.error;
        console.error = (...args) => {
          if (String(args[0] ?? '').includes('failed to draw')) failures.push(args.map(String).join(' '));
          else realError2(...args);
        };
        try {
          api.render(snapshot);
        } finally {
          console.error = realError2;
        }
      }
    },
  };
}

const problems = [];
const note = (m) => problems.push(m);

// --- 1. the whole dashboard draws against a real snapshot -------------------
const snapshot = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
const main = newEnv();
main.draw(snapshot, 3);

for (const f of new Set(main.failures)) note(f);

const cols = main.columns();
if (!cols.size) note('no version columns were drawn');

let tickets = 0;
let stuck = 0;
const shape = [];
for (const [label, stack] of cols) {
  tickets += stack.children.length;
  stuck += stack.children.filter((t) => t.className.includes('otick-stuck')).length;
  shape.push(`${label}:${stack.children.length}`);
}
if (!tickets) note('columns were drawn but hold no orders');

const wantStuck = (snapshot.orders || []).filter((o) => o.degraded).length;
if (stuck !== wantStuck) note(`stuck tickets = ${stuck}, want ${wantStuck}`);

// Columns reserve cap * card height, so the cap has to reach the stylesheet or
// a full column and an idle one would be different heights and the page would
// grow and shrink as work arrives.
if (!main.html.props['--column-cap']) {
  note('--column-cap was never published; the reserved column height cannot match the cap');
}

// --- 2. only the front card of a column leaves ------------------------------
//
// A finished order further down greys out and waits, so a run of them reads as
// a block moving up to the front. Getting this wrong is what made the first
// version look jumpy: cards vanished from the middle of the stack, which is
// invisible when every card looks alike.
function order(id, version, done) {
  return {
    orderId: id, version, step: 'Prep', status: done ? 'Completed' : 'Running',
    degraded: false, done, elapsedSec: 4,
  };
}

const staged = JSON.parse(JSON.stringify(snapshot));
staged.orders = [
  // v1: two finished at the front, then a live one.
  order('ord-000001', 'v1', true),
  order('ord-000002', 'v1', true),
  order('ord-000003', 'v1', false),
  // v2: a live order at the front, with a finished one behind it.
  order('ord-000010', 'v2', false),
  order('ord-000011', 'v2', true),
];

// Drawn twice on purpose. Snapshots arrive about once a second while a
// departure takes 420ms, so a departing card is re-rendered mid-flight and
// must not be sent on its way a second time.
const dep = newEnv();
dep.draw(staged, 2);

const depCols = dep.columns();
const cls = (stack, i) => (stack.children[i] ? stack.children[i].className : '<missing>');

const v1 = depCols.get('v1');
if (!cls(v1, 0).includes('otick-departing')) note(`v1 front card is finished but not departing: "${cls(v1, 0)}"`);
if (cls(v1, 1).includes('otick-departing')) note('v1 second card is departing; only the front card may leave');
if (!cls(v1, 1).includes('otick-done')) note(`v1 second card is finished but not greyed: "${cls(v1, 1)}"`);

const v2 = depCols.get('v2');
if (cls(v2, 0).includes('otick-departing')) note('v2 front card is still running but is departing');
if (cls(v2, 1).includes('otick-departing')) note('v2 finished card departed from behind a running order');
if (!cls(v2, 1).includes('otick-done')) note(`v2 finished card is not greyed: "${cls(v2, 1)}"`);

if (dep.api.departing.get('v1') !== 'ord-000001') {
  note(`v1 is departing ${dep.api.departing.get('v1')}, want the front card`);
}
if (dep.api.departing.has('v2')) note('v2 has a departure in flight with a running order at the front');
if (dep.departures.length !== 1) {
  note(`${dep.departures.length} departures started across two renders, want 1 — ` +
    'a card already leaving must not be restarted');
}

// --- 3. the display cap is per column, not shared ---------------------------
//
// A version with forty orders in flight must not squeeze a quieter version's
// two cards off the screen. (That the *sample* is also per version is enforced
// in Go, by TestOrderSampleIsScopedPerVersionAndToRunningOrders — a fixture
// here is handed both versions whatever the backend did, so it cannot see it.)
const burst = JSON.parse(JSON.stringify(snapshot));
burst.orders = [
  order('ord-000001', 'v2', false),
  order('ord-000002', 'v2', false),
  ...Array.from({ length: 40 }, (_, i) => order(`ord-10${String(i).padStart(4, '0')}`, 'v3', false)),
];

const rush = newEnv();
rush.draw(burst, 2);
const rushCols = rush.columns();
if (!rushCols.get('v2') || rushCols.get('v2').children.length !== 2) {
  note(`v2 column holds ${rushCols.get('v2') ? rushCols.get('v2').children.length : 0} cards ` +
    'after a v3 burst, want its own 2 — a version must keep its own sample');
}

// --- 4. an order that stops being reported departs, it is not yanked --------
//
// The sample holds running orders only, so an order completing simply stops
// being reported. If that is treated as "gone" rather than "finished", the
// card is removed from wherever it sits — which is the jump this whole model
// exists to avoid.
const before = JSON.parse(JSON.stringify(snapshot));
before.orders = [
  order('ord-000001', 'v1', false),
  order('ord-000002', 'v1', false),
  order('ord-000003', 'v1', false),
];
const after = JSON.parse(JSON.stringify(snapshot));
// The front order has finished, so the backend no longer reports it.
after.orders = before.orders.slice(1);

const gone = newEnv();
gone.draw(before);
gone.draw(after);

const goneCol = gone.columns().get('v1');
if (!goneCol || goneCol.children.length !== 3) {
  note(`v1 holds ${goneCol ? goneCol.children.length : 0} cards after the front order finished, ` +
    'want 3 — the finished card must stay while it plays out');
} else if (!cls(goneCol, 0).includes('otick-departing')) {
  note(`the finished front card is not departing: "${cls(goneCol, 0)}" — it was yanked instead`);
}
if (gone.api.departing.get('v1') !== 'ord-000001') {
  note(`v1 is departing ${gone.api.departing.get('v1')}, want the order that stopped being reported`);
}

// --- report -----------------------------------------------------------------
if (problems.length) {
  for (const p of problems) console.error('  ' + p);
  console.error(`${problems.length} problem(s)`);
  process.exit(1);
}

console.log(`render OK — ${cols.size} columns [${shape.join(' ')}], ${tickets} tickets, ${stuck} stuck`);
console.log(`departure OK — front card leaves, finished cards behind it wait and grey ` +
  `(cap ${main.html.props['--column-cap']})`);
console.log('isolation OK — a burst on one version leaves another version\'s column alone');
console.log('completion OK — an order that stops being reported plays its departure');
