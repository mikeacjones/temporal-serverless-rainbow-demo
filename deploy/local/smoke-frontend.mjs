// Run the real frontend renderer against a captured snapshot, in node.
//
// check-frontend.sh catches names that do not resolve; this catches code that
// resolves and then throws. That gap has been expensive: a renderer that threw
// mid-frame left three panels blank on screen while every static check passed,
// and it happened more than once.
//
// The DOM below is the smallest thing app.js will accept. It is not a browser
// and is not trying to be — it exists so that render() runs for real against
// real data and any exception fails the build.
import fs from 'node:fs';
import path from 'node:path';

const root = path.resolve(import.meta.dirname, '../..');
const fixture = process.argv[2] || path.join(root, 'deploy/local/fixtures/snapshot.json');

const ids = new Map();

class Element {
  constructor(tag) {
    this.tagName = tag;
    this.children = [];
    this.dataset = {};
    this.style = { setProperty() {} };
    this.title = '';
    this._text = '';
    this._cls = new Set();
  }
  get className() { return [...this._cls].join(' '); }
  set className(v) { this._cls = new Set(String(v).split(/\s+/).filter(Boolean)); }
  get classList() {
    const s = this._cls;
    return {
      add: (c) => s.add(c),
      remove: (c) => s.delete(c),
      contains: (c) => s.has(c),
      toggle: (c, on) => (on ? s.add(c) : s.delete(c)),
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
    // class has to reach the class set, not sit on the element as a plain
    // property, or a selector search can never find what was just created.
    if (k === 'class') this.className = v;
    else this[k] = v;
  }
  getAttribute(k) { return k === 'class' ? this.className : this[k]; }
  addEventListener() {}
  querySelectorAll(sel) {
    const found = [];
    this._walk(sel, found);
    return found;
  }
  // Only class selectors are used, and they have to actually match: the
  // sparkline code builds its SVG on the first miss and then reads the parts
  // back by class, so a stub that always answers null or always answers an
  // empty element breaks in opposite directions.
  querySelector(sel) { return this.querySelectorAll(sel)[0] ?? null; }
  _walk(sel, out) {
    const want = String(sel).startsWith('.') ? String(sel).slice(1) : null;
    for (const kid of this.children) {
      if (want && kid._cls.has(want)) out.push(kid);
      kid._walk(sel, out);
    }
  }
  getBoundingClientRect() { return { top: 0, height: 42 }; }
  get firstChild() { return this.children[0] ?? null; }
  get parentNode() { return null; }
}

const document = {
  createElement: (t) => new Element(t),
  createElementNS: (_ns, t) => new Element(t),
  createTextNode: (t) => {
    const node = new Element('#text');
    node.textContent = String(t);
    return node;
  },
  getElementById(id) {
    if (!ids.has(id)) ids.set(id, new Element('div'));
    return ids.get(id);
  },
  querySelectorAll: () => [],
  querySelector: () => null,
  addEventListener() {},
  body: new Element('body'),
};

globalThis.document = document;
globalThis.window = { location: { origin: 'http://localhost', pathname: '/' }, addEventListener() {} };
globalThis.location = globalThis.window.location;
globalThis.EventSource = class { addEventListener() {} close() {} };
globalThis.requestAnimationFrame = (fn) => fn();
// The page fetches once on load; the smoke test drives render() itself.
globalThis.fetch = () => Promise.reject(new Error('smoke test: no network'));

// render() deliberately isolates each panel, so a broken panel logs rather
// than throwing — which is exactly how a blank panel shipped before. The
// harness therefore fails on the log, not on an exception.
const failures = [];
const realError = console.error;
console.error = (...args) => {
  const first = String(args[0] ?? '');
  if (first.includes('failed to draw')) failures.push(args.map(String).join(' '));
  else realError(...args);
};

const src = fs.readFileSync(path.join(root, 'frontend/app.js'), 'utf8');
const load = new Function(`${src}\nreturn { render };`);

let api;
try {
  api = load();
} catch (err) {
  console.error('app.js failed to load:', err.message);
  process.exit(1);
}

const snapshot = JSON.parse(fs.readFileSync(fixture, 'utf8'));

// Three frames: the first seats, the second updates what is seated, the third
// runs after finished orders have been marked leaving.
for (let frame = 1; frame <= 3; frame += 1) {
  try {
    api.render(snapshot);
  } catch (err) {
    console.error(`render() threw on frame ${frame}:`, err.stack || err.message);
    process.exit(1);
  }
}

console.error = realError;

if (failures.length) {
  for (const f of new Set(failures)) console.error(f);
  console.error(`${new Set(failures).size} panel(s) failed to draw`);
  process.exit(1);
}

// A panel that draws nothing is just as broken as one that throws.
const columns = document.getElementById('order-columns').children;
if (!columns.length) {
  console.error('no version columns were drawn');
  process.exit(1);
}

let tickets = 0;
let stuck = 0;
const shape = [];
for (const col of columns) {
  const [head, stack] = col.children;
  tickets += stack.children.length;
  stuck += stack.children.filter((t) => t.className.includes('otick-stuck')).length;
  shape.push(`${head.children[0].textContent}:${stack.children.length}`);
}

const expectedStuck = (snapshot.orders || []).filter((o) => o.degraded).length;
if (!tickets) {
  console.error('columns were drawn but hold no orders');
  process.exit(1);
}
if (stuck !== expectedStuck) {
  console.error(`stuck tickets = ${stuck}, want ${expectedStuck}`);
  process.exit(1);
}

console.log(`render OK — ${columns.length} columns [${shape.join(' ')}], ` +
  `${tickets} tickets, ${stuck} stuck`);
