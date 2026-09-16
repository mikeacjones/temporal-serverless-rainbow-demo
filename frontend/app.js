/* Order ops console.
 *
 * A pure renderer: the backend sends a complete snapshot every second over
 * Server-Sent Events, and this file draws it. There is no client-side model of
 * the world to drift out of step with the server, which matters when the thing
 * on screen is being used to make deploy decisions in front of an audience.
 *
 * The one piece of local state is which rail filter is selected, because that
 * is a property of the viewer rather than of the system. */

'use strict';

// The API is same-origin in the containerised setup, where nginx proxies /api
// and /events to the backend. ?api=http://host:port points a dev build at a
// backend running elsewhere.
const API = new URLSearchParams(location.search).get('api') || '';
const url = (path) => API + path;

// Five frequencies of the spectrum, one per version, used for nothing else.
// Cyan means live and crimson means trouble, so neither is ever a version and
// a version's identity can never imply its health.
const VERSION_COLOR = {
  v1: 'var(--v1)', v2: 'var(--v2)', v3: 'var(--v3)',
  v4: 'var(--v4)', v5: 'var(--v5)',
};
const colorFor = (label) => VERSION_COLOR[label] || 'var(--ink-faint)';

const $ = (id) => document.getElementById(id);

let snapshot = null;
let railFilter = 'all';

/* --- Formatting --------------------------------------------------------- */

const int = (n) => Math.round(Number(n) || 0).toLocaleString('en-US');

const pct = (n) => {
  const v = Number(n) || 0;
  // Sub-1% ramps are the whole point of a canary, so don't round them to zero.
  return (v > 0 && v < 1 ? v.toFixed(1) : Math.round(v)) + '%';
};

const age = (seconds) => {
  const s = Math.max(0, Math.round(Number(seconds) || 0));
  if (s < 60) return s + 's';
  const m = Math.floor(s / 60);
  return m < 60 ? m + 'm ' + (s % 60) + 's' : Math.floor(m / 60) + 'h ' + (m % 60) + 'm';
};

/* --- Requests ----------------------------------------------------------- */

async function post(path, body) {
  const response = await fetch(url(path), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  const text = await response.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { /* not JSON */ }

  if (!response.ok) {
    throw new Error((data && data.error) || text || response.statusText);
  }
  return data;
}

// act runs an action and reports the outcome, so a rejected control action
// always says why rather than appearing to do nothing.
async function act(path, body, success) {
  try {
    const data = await post(path, body);
    if (success) toast(success(data));
    return data;
  } catch (err) {
    toast(String(err.message || err), true);
    return null;
  }
}

function toast(message, isError) {
  const el = document.createElement('div');
  el.className = 'toast' + (isError ? ' toast-error' : '');
  el.textContent = message;
  $('toasts').append(el);
  setTimeout(() => el.remove(), isError ? 8000 : 4000);
}

/* --- Live connection ---------------------------------------------------- */

function connect() {
  const events = new EventSource(url('/events'));

  events.addEventListener('open', () => link('live', 'live'));

  events.addEventListener('message', (event) => {
    try {
      render(JSON.parse(event.data));
      link('live', 'live');
    } catch (err) {
      console.error('bad snapshot', err);
    }
  });

  events.addEventListener('error', () => {
    link('lost', 'reconnecting');
    events.close();
    // The browser would reconnect on its own, but only after closing; doing it
    // explicitly keeps the retry interval predictable during a demo.
    setTimeout(connect, 2000);
  });
}

function link(state, text) {
  $('link-dot').dataset.state = state;
  $('link-text').textContent = text;
}

/* --- Render ------------------------------------------------------------- */

// render draws one snapshot, panel by panel.
//
// Each panel is isolated: a throw in one used to abort the whole function, so
// a single bad reference silently emptied every panel drawn after it — the
// version cards, the rail and the fault controls all at once, with nothing on
// screen saying why. A dashboard being used to make deploy decisions should
// lose one panel at most, and say so in the console.
function render(next) {
  snapshot = next;

  for (const [name, draw] of [
    ['readouts', renderReadouts],
    ['routing', renderSpectrum],
    ['legend', renderLegend],
    ['deployment', renderRollout],
    ['fleet', renderFleet],
    ['versions', renderStations],
    ['rail', renderRail],
    ['faults', renderFault],
  ]) {
    try {
      draw(next);
    } catch (err) {
      console.error(`panel "${name}" failed to draw`, err);
    }
  }
}

function renderReadouts(s) {
  const traffic = s.traffic;
  $('rate-actual').textContent = traffic && traffic.running ? int(traffic.ratePerMin) : '0';
  $('throughput').textContent = int(s.completedPerMin);
  $('inflight').textContent = int(s.totals.running);
  $('backlog-top').textContent = int(s.capacity.backlogDepth);
  $('order-time').textContent = medianOrderTime(s.orders);
  renderAutoStop(traffic);

  const stuck = s.totals.degraded || 0;
  $('stuck-readout').hidden = stuck === 0;
  $('stuck').textContent = int(stuck);

  // Keep the rate box in step with the server, but never while it is being
  // typed into.
  const input = $('rate-input');
  if (document.activeElement !== input && traffic) {
    input.value = traffic.running ? traffic.ratePerMin : 0;
  }

  const current = traffic && traffic.running ? Number(traffic.ratePerMin) : 0;
  for (const chip of document.querySelectorAll('.chip[data-rate]')) {
    chip.setAttribute('aria-pressed', String(Number(chip.dataset.rate) === current));
  }
}

// medianOrderTime reads the middle completed order out of the sampled strip.
//
// A median rather than a p99: the sample is the most recent few dozen orders,
// which is nowhere near enough to place a tail percentile honestly.
//
// This one really does want Completed rather than order.done: a terminated or
// failed order stopped partway, so its elapsed time is not how long an order
// takes.
function medianOrderTime(orders) {
  const done = (orders || [])
    .filter((o) => o.status === 'Completed')
    .map((o) => o.elapsedSec)
    .sort((a, b) => a - b);
  return done.length ? age(done[Math.floor(done.length / 2)]) : '—';
}

// renderAutoStop says when traffic will stop itself.
//
// Worth showing rather than hiding: with serverless workers, traffic left
// running keeps invoking workers, so the generator stops on its own — and an
// operator who does not know that would think the demo broke.
function renderAutoStop(traffic) {
  const note = $('autostop');
  if (!traffic) {
    note.hidden = true;
    return;
  }

  if (traffic.autoStopped) {
    note.hidden = false;
    note.textContent = 'Flow stopped itself after being left untouched. Set a rate to start again.';
    return;
  }

  const stopAt = traffic.stopAt ? new Date(traffic.stopAt) : null;
  if (!stopAt || !traffic.ratePerMin || !traffic.running) {
    note.hidden = true;
    return;
  }

  const seconds = Math.round((stopAt - Date.now()) / 1000);
  note.hidden = seconds > 300; // only worth saying when it is close
  if (!note.hidden) note.textContent = `Flow stops itself in ${age(seconds)} unless you change something.`;
}

// renderSpectrum draws the three hero bars.
//
// The gap between the first two is the pinning guarantee: new orders move on a
// deploy, in-flight orders do not. The third is empty at rest and fills per
// version as work arrives, which is the serverless story.
function renderSpectrum(s) {
  const routing = s.deployment.routing || {};
  const versions = s.deployment.versions || [];

  // Version cards already carry the effective share, which is what a traffic
  // split rewrites — so reading them covers both routing and a hand-set split.
  const routed = versions
    .map((v) => ({ label: v.label, share: v.trafficPct || 0 }))
    .filter((v) => v.share > 0);
  drawBar($('split-routing'), routed, 'No version is taking orders yet', (x) => pct(x.share));

  const inFlight = versions
    .map((v) => ({ label: v.label, share: (s.health[v.label] || {}).running || 0 }))
    .filter((v) => v.share > 0);
  drawBar($('split-inflight'), inFlight, 'No orders in flight', (x) => int(x.share));


  // The shift chip names the move a live rollout is making.
  const rollout = s.rollout;
  const live = rollout && LIVE_PHASES.has(rollout.phase);
  $('shift').hidden = !live;
  if (live) {
    $('shift-from').textContent = rollout.previousCurrentLabel || routing.currentLabel || '—';
    $('shift-to').textContent = rollout.targetVersion;
    $('shift-from').style.color = colorFor(rollout.previousCurrentLabel || routing.currentLabel);
    $('shift-to').style.color = colorFor(rollout.targetVersion);
  }
}

function drawBar(target, shares, emptyMessage, caption) {
  const total = shares.reduce((sum, s) => sum + s.share, 0);
  if (total <= 0) {
    target.replaceChildren(el('span', { class: 'bar-empty', text: emptyMessage }));
    return;
  }

  target.replaceChildren(...shares.map((share) => {
    const segment = el('div', { class: 'seg' });
    segment.style.background = colorFor(share.label);
    segment.style.flex = `${share.share} 1 0`;
    segment.append(
      el('span', { class: 'seg-name', text: share.label }),
      el('span', { text: caption(share) }),
    );
    return segment;
  }));
}

// renderLegend names each version's routing role, in spectrum order.
function renderLegend(s) {
  const routing = s.deployment.routing || {};
  const versions = s.deployment.versions || [];

  const chips = versions.map((v) => {
    const role = v.label === routing.currentLabel ? 'current'
      : v.label === routing.rampingLabel ? 'ramping'
      : v.status === 'draining' ? 'draining'
      : v.trafficPct > 0 ? 'serving'
      : 'idle';

    const chip = el('span', {
      class: 'legend-chip' + (role === 'idle' ? '' : ' legend-chip-live'),
      text: '',
    });
    chip.style.setProperty('--version-color', colorFor(v.label));
    chip.append(
      el('span', { class: 'legend-dot' }),
      el('span', { text: `${v.label} ${role}` }),
    );
    return chip;
  });

  const traffic = s.traffic;
  const inbound = traffic && traffic.running ? traffic.ratePerMin : 0;
  chips.push(el('span', {
    class: 'legend-total',
    text: `total ingestion: ${int(inbound)} orders/min`,
  }));

  $('spectrum-legend').replaceChildren(...chips);
}

const PHASE_TEXT = {
  pending: 'starting',
  gating: 'checking the new version',
  ramping: 'ramping',
  paused: 'paused',
  promoting: 'promoting',
  completed: 'done',
  rolledBack: 'rolled back',
  aborted: 'stopped',
  gateFailed: 'blocked before any traffic moved',
};

const BAD_PHASES = new Set(['rolledBack', 'aborted', 'gateFailed']);
const LIVE_PHASES = new Set(['pending', 'gating', 'ramping', 'paused', 'promoting']);

function renderRollout(s) {
  const body = $('rollout-body');
  const controls = $('spectrum-controls');
  const rollout = s.rollout;

  if (!rollout || !rollout.targetVersion) {
    controls.replaceChildren();
    body.replaceChildren(el('p', {
      class: 'rollout-empty',
      text: 'No deployment running. Pick a version below and start one, or send orders straight to any version.',
    }));
    return;
  }

  const live = LIVE_PHASES.has(rollout.phase);
  controls.replaceChildren(...(live ? rolloutControls(rollout) : []));

  const parts = [];

  const top = el('div', { class: 'rollout-top' });
  const target = el('span', { class: 'rollout-target', text: rollout.targetVersion });
  target.style.color = colorFor(rollout.targetVersion);
  top.append(
    target,
    el('span', {
      class: 'chip-phase phase-' + rollout.phase.toLowerCase(),
      text: PHASE_TEXT[rollout.phase] || rollout.phase,
    }),
  );
  parts.push(top);

  parts.push(renderStages(rollout));

  if (rollout.message) {
    parts.push(el('p', {
      class: 'rollout-msg' + (BAD_PHASES.has(rollout.phase) ? ' rollout-msg-bad' : ''),
      text: rollout.message,
    }));
  }

  parts.push(renderRolloutStats(rollout));

  if (rollout.manualControl) {
    parts.push(el('p', {
      class: 'manual-flag',
      text: 'You set this share by hand. It holds here, still watching for trouble, until you continue or stop it.',
    }));
  }

  body.replaceChildren(...parts);
}

function renderStages(rollout) {
  const rail = el('div', { class: 'stages' });
  const stages = rollout.stages || [];
  const live = LIVE_PHASES.has(rollout.phase);

  // The gate is the first step of the sequence, and the only one that can
  // stop a deployment before any customer is affected.
  const gateRan = rollout.gate && rollout.gate.ran;
  const gateState = rollout.phase === 'gating' ? 'stage-live'
    : gateRan && rollout.gate.passed ? 'stage-done'
    : rollout.phase === 'gateFailed' ? 'stage-live' : '';

  const gate = el('div', { class: 'stage stage-gate ' + gateState, text: 'check' });
  gate.append(el('span', {
    class: 'stage-sub',
    text: rollout.phase === 'gateFailed' ? 'failed' : gateRan && rollout.gate.passed ? 'passed' : 'canary',
  }));
  rail.append(gate);

  // Key the rail off the percentage actually in force, not off stageIndex.
  //
  // Those are not the same thing: after an operator sets a share by hand,
  // stageIndex points at the next *unrun* stage, so highlighting it showed
  // "50% holding" while the ramp — and every other number — was at 25%.
  const current = rollout.currentPct;
  const onAStage = stages.some((stage) => stage.pct === current);

  stages.forEach((stage) => {
    let state = '';
    if (stage.pct < current) state = 'stage-done';
    else if (stage.pct === current) state = live ? 'stage-live' : 'stage-done';

    const node = el('div', { class: 'stage ' + state, text: pct(stage.pct) });
    if (state === 'stage-live') {
      if (rollout.holdRemainingSec > 0) {
        node.append(el('span', { class: 'stage-sub', text: age(rollout.holdRemainingSec) + ' left' }));
      } else if (rollout.holdRemainingSec < 0) {
        node.append(el('span', { class: 'stage-sub', text: 'holding' }));
      }
    }
    rail.append(node);
  });

  // An operator can set any share, not only one the plan lists.
  if (live && !onAStage && current > 0) {
    const manual = el('div', { class: 'stage stage-live stage-manual', text: pct(current) });
    manual.append(el('span', { class: 'stage-sub', text: 'by hand' }));
    rail.append(manual);
  }

  return rail;
}

function renderRolloutStats(rollout) {
  const row = el('div', { class: 'rollout-stats' });
  const health = rollout.health || {};
  const bad = (health.samples || 0) > 0 && health.errorRatePct > (rollout.policy || {}).maxErrorRatePct;

  row.append(
    stat(pct(rollout.currentPct), 'of new orders'),
    stat(
      health.samples ? pct(health.errorRatePct) : '—',
      health.samples ? `failed or stuck of ${int(health.samples)}` : 'no orders judged yet',
      bad,
    ),
  );
  if (rollout.gate && rollout.gate.ran) {
    row.append(stat(
      `${rollout.gate.probes - rollout.gate.failures}/${rollout.gate.probes}`,
      'canary orders passed',
      !rollout.gate.passed,
    ));
  }
  return row;
}

function stat(value, name, bad) {
  const node = el('div', { class: 'stat' + (bad ? ' stat-bad' : '') });
  node.append(
    el('span', { class: 'stat-value', text: value }),
    el('span', { class: 'stat-name', text: name }),
  );
  return node;
}

// rolloutControls sit in the routing header, beside the bars they move.
function rolloutControls(rollout) {
  const nodes = [];

  if (rollout.phase === 'paused') {
    nodes.push(button('Continue', 'btn-primary', () =>
      act('/api/rollout/resume', undefined, () => 'Deployment continuing')));
  } else {
    nodes.push(button(`Hold ${pct(rollout.currentPct)}`, '', () =>
      act('/api/rollout/pause', undefined, () => 'Deployment paused')));
  }

  nodes.push(button('Next stage', '', () =>
    act('/api/rollout/advance', undefined, () => 'Moved to the next stage')));

  const jump = el('div', { class: 'rollout-jump' });
  const input = el('input', { type: 'number', min: '0', max: '100', step: '5' });
  input.value = Math.round(rollout.currentPct);
  input.setAttribute('aria-label', 'Share of new orders');
  jump.append(input, button('Set share', '', () =>
    act('/api/rollout/ramp', { pct: Number(input.value) },
      (state) => `Sending ${pct(state.currentPct)} of new orders to ${state.targetVersion}`)));
  nodes.push(jump);

  nodes.push(button(`Abort to ${rollout.previousCurrentLabel || 'current'}`, 'btn-danger', () =>
    act('/api/rollout/abort', { rollback: true }, () => 'Rolled back')));

  return nodes;
}

function renderFleet(s) {
  const capacity = s.capacity || {};
  $('workers').textContent = int(capacity.workers);
  $('workers').className = 'gauge-value' + (capacity.workers ? ' gauge-value-live' : '');
  $('backlog').textContent = int(capacity.backlogDepth);

  // Zero wait is the good case and deserves to read as such.
  const wait = capacity.oldestWaitSec || 0;
  $('oldest-wait').textContent = wait < 1 ? 'none' : age(wait);

  renderSyncMatch(s.syncMatch || {});

  const history = s.history || {};
  drawSpark($('spark-workers'), history.workers, 'var(--cyan)');
  drawSpark($('spark-backlog'), history.backlog, 'var(--v4)');

  // Name the peak over the window the graph actually shows, so the figure is
  // scoped rather than implied.
  const peakWorkers = peakOf(history.workers);
  $('workers-name').textContent = peakWorkers > (capacity.workers || 0)
    ? `workers running · peak ${int(peakWorkers)}`
    : 'workers running';

  const peakBacklog = peakOf(history.backlog);
  $('backlog-name').textContent = peakBacklog > (capacity.backlogDepth || 0)
    ? `backlog task queue · peak ${int(peakBacklog)}`
    : 'backlog task queue';
}

// renderSyncMatch shows the real sync match rate: the share of tasks the
// server handed straight to a worker that was already waiting. This is the
// signal Temporal scales on.
//
// Unlike the other gauges it can be genuinely unknown — it comes from the
// server's metrics endpoint — and saying so beats a confident zero.
function renderSyncMatch(syncMatch) {
  const known = syncMatch.ratePct >= 0;
  const value = $('syncmatch');

  value.textContent = known ? pct(syncMatch.ratePct) : '—';
  value.className = 'gauge-value' + (known && syncMatch.ratePct >= 95 ? ' gauge-value-live' : '');

  if (!known) {
    $('syncmatch-name').textContent = syncMatch.available
      ? 'handed straight over — no tasks yet'
      : 'handed straight over — metrics unavailable';
    return;
  }

  // Say how many tasks the figure is based on: 100% of four tasks and 100% of
  // four hundred are not the same claim.
  $('syncmatch-name').textContent = syncMatch.delivered > 0
    ? `handed straight over, of ${int(syncMatch.delivered)}/s`
    : 'handed straight over';
}

// spark draws a small scrolling sparkline.
//
// It reuses its nodes and slides rather than redrawing. Each tick the series
// has shifted one sample to the left, so the line is placed one sample to the
// right with no transition and then travelled back to zero over the tick —
// which reads as continuous movement instead of a jump every second.
// Recreating the nodes would reset that transition, and the pulse on the
// leading edge, every frame.
function drawSpark(svg, series, color) {
  if (!series || series.length < 2) {
    svg.replaceChildren();
    return;
  }

  // Dimensions come from the element's own viewBox, so the same routine draws
  // the taller fleet gauges and the shorter version-card graphs.
  const [, , width, height] = (svg.getAttribute('viewBox') || '0 0 120 30')
    .split(/\s+/).map(Number);
  const max = Math.max(...series, 1);
  const step = width / (series.length - 1);

  const points = series.map((value, i) => {
    const x = i * step;
    const y = height - (Math.max(0, value) / max) * (height - 4) - 2;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  });

  let shift = svg.querySelector('.spark-shift');
  if (!shift) {
    shift = svgEl('g', { class: 'spark-shift' });
    shift.append(
      svgEl('polygon', { class: 'spark-area' }),
      svgEl('polyline', { class: 'spark-line' }),
    );
    svg.replaceChildren(shift, svgEl('circle', { class: 'spark-head', r: '2' }));
  }

  const [area, line] = shift.children;
  area.setAttribute('points', `0,${height} ${points.join(' ')} ${width},${height}`);
  line.setAttribute('points', points.join(' '));
  area.style.fill = `color-mix(in srgb, ${color} 16%, transparent)`;
  line.style.stroke = color;

  // The leading edge marks "now" and stays put while the line moves under it.
  const head = svg.querySelector('.spark-head');
  const lastY = height - (Math.max(0, series.at(-1)) / max) * (height - 4) - 2;
  head.setAttribute('cx', String(width));
  head.setAttribute('cy', lastY.toFixed(2));
  head.style.fill = color;

  shift.classList.remove('spark-shift-animate');
  shift.style.transform = `translateX(${step}px)`;
  // Force the shifted position to be applied before the transition starts,
  // or the browser collapses both changes into one and nothing moves.
  void shift.getBoundingClientRect();
  shift.classList.add('spark-shift-animate');
  shift.style.transform = 'translateX(0)';
}

// peakOf reports the highest value in the visible history window.
//
// This is a real peak over a known window, unlike the "peak worker count"
// label this replaces — that was simply the live count over time, labelled as
// something it was not.
function peakOf(series) {
  return series && series.length ? Math.max(...series) : 0;
}

// renderStations reconciles the version cards by label rather than rebuilding
// them.
//
// Rebuilding every second had two costs beyond the wasted work. The burst-size
// input was recreated each frame, so a typed value was wiped about a second
// later and only the default could actually be used. And a sparkline inside a
// card that is thrown away cannot animate, for the same reason the order
// tickets could not.
function renderStations(s) {
  const routing = s.deployment.routing || {};
  const versions = s.deployment.versions || [];
  const container = $('stations');

  $('roster-count').textContent = `${versions.length} registered`;

  const existing = new Map();
  for (const node of container.children) existing.set(node.dataset.version, node);

  container.replaceChildren(...versions.map((version) => {
    const node = existing.get(version.label) || stationShell(version.label);
    return updateStation(node, version, s, routing);
  }));
}

// stationShell builds the parts of a card that never change, so that updates
// only have to write text and classes.
function stationShell(label) {
  const station = el('div', { class: 'station' });
  station.dataset.version = label;
  station.style.setProperty('--version-color', colorFor(label));

  const head = el('div', { class: 'station-head' });

  const nameRow = el('div', { class: 'station-name-row' });
  nameRow.append(
    el('span', { class: 'station-name', text: label }),
    el('span', { class: 'station-role' }),
  );

  const share = el('div', { class: 'station-share' });
  share.append(
    el('span', { class: 'station-share-value' }),
    el('span', { class: 'station-share-name', text: 'of new orders' }),
  );

  const workers = el('div', { class: 'station-workers' });
  workers.append(
    el('span', { class: 'station-workers-value' }),
    el('span', { class: 'station-workers-name' }),
  );

  // Its own workers over time, under its own count.
  const sparkSvg = svgEl('svg', {
    class: 'spark spark-station',
    viewBox: '0 0 120 26',
    preserveAspectRatio: 'none',
    'aria-hidden': 'true',
  });

  head.append(nameRow, share, workers, sparkSvg, el('div', { class: 'station-counts' }));
  station.append(head, el('div', { class: 'station-foot' }));
  return station;
}

function updateStation(station, version, s, routing) {
  const health = s.health[version.label] || {};
  const stuck = health.degraded || 0;
  const serving = version.trafficPct > 0;

  station.className = 'station'
    + (stuck > 0 ? ' station-trouble' : serving ? ' station-active' : '');

  const [head, foot] = station.children;
  const [nameRow, share, workers, sparkSvg, counts] = head.children;

  const role = nameRow.children[1];
  role.className = 'station-role station-role-' + version.status;
  role.textContent = roleText(version);

  share.children[0].textContent = pct(version.trafficPct);

  workers.className = 'station-workers' + (version.workers ? ' station-workers-live' : '');
  workers.children[0].textContent = int(version.workers || 0);
  workers.children[1].textContent = version.workers ? 'workers running' : 'no workers running';

  drawSpark(sparkSvg, ((s.history || {}).workersByVersion || {})[version.label], colorFor(version.label));

  // The pipeline is no longer listed step by step here — every order ticket
  // draws it as dots — but its length is what differs between versions, so it
  // stays as a number.
  const steps = ((s.pipelines || {})[version.label] || []).length;
  const parts = [];
  if (steps) parts.push(countNode('steps', steps));
  parts.push(countNode('in flight', health.running || 0));
  parts.push(countNode('served', health.completed || 0));
  if (stuck > 0) {
    const node = countNode('stuck', stuck);
    node.className = 'station-stuck';
    parts.push(node);
  }
  counts.replaceChildren(...parts);

  updateStationFoot(foot, version, s, routing, health, stuck);
  return station;
}

// updateStationFoot rewrites the buttons only when the set of them changes, so
// the burst-size input keeps whatever has been typed into it.
function updateStationFoot(foot, version, s, routing, health, stuck) {
  const rolloutLive = s.rollout && LIVE_PHASES.has(s.rollout.phase);
  const isCurrent = version.label === routing.currentLabel;
  const canRescue = stuck > 0 && !isCurrent;

  // A signature of *which* controls belong here, deliberately excluding any
  // number that ticks. Including the rescue count would rebuild the foot every
  // second whenever orders were stranded — clobbering the burst-size input at
  // exactly the moment an operator is most likely to be typing into it.
  const shape = [isCurrent, canRescue, routing.currentLabel].join('|');

  if (foot.dataset.shape !== shape) {
    foot.dataset.shape = shape;

    const parts = [];
    if (isCurrent) {
      parts.push(el('span', { class: 'station-role station-role-current', text: 'Taking orders now' }));
    } else {
      parts.push(button('Start deployment', 'btn-primary', () =>
        act('/api/rollout', { targetVersion: version.label },
          (state) => `Deploying ${state.targetVersion}: checking it first`)));
    }

    // Send orders straight here, whatever the routing says. Aimed at idle
    // versions especially: they have no workers running, so the burst makes
    // serverless workers appear from nothing.
    parts.push(dumpControl(version.label));

    if (canRescue) {
      const move = button(`Move ${int(health.running || 0)} to ${routing.currentLabel}`, 'btn-rescue', () =>
        act('/api/orders/recover', { version: version.label }, (data) => data.message));
      move.title = `Restart every order still running on ${version.label}, pinned to ${routing.currentLabel}`;
      parts.push(move);
    }
    foot.replaceChildren(...parts);
  }

  // Anything that ticks is written in place instead.
  const start = foot.querySelector('.btn-primary');
  if (start) {
    start.disabled = rolloutLive;
    start.title = rolloutLive ? 'A deployment is already running' : '';
  }

  const rescue = foot.querySelector('.btn-rescue');
  if (rescue) {
    rescue.textContent = `Move ${int(health.running || 0)} to ${routing.currentLabel}`;
  }
}

// dumpControl is the per-version burst control: a count and a button.
//
// Its input is why the cards are reconciled rather than rebuilt — recreating
// this node every second wiped whatever had been typed into it.
function dumpControl(label) {
  const row = el('div', { class: 'dump' });

  const count = el('input', { type: 'number', min: '1', max: '20000', step: '50', class: 'dump-count' });
  count.value = 250;
  count.setAttribute('aria-label', `Orders to send straight to ${label}`);

  const send = button(`Send to ${label}`, '', () =>
    act('/api/versions/dump', { version: label, count: Number(count.value) },
      () => `Sent ${int(count.value)} orders straight to ${label}`));
  send.title = `Start orders pinned to ${label}, ignoring the routing split`;

  row.append(count, send);
  return row;
}

function roleText(version) {
  switch (version.status) {
    case 'current': return 'taking new orders';
    case 'ramping': return 'being deployed';
    case 'draining': return 'finishing its orders';
    default: return version.trafficPct > 0 ? 'serving' : 'ready, idle';
  }
}

function countNode(name, value) {
  const node = el('span', {});
  node.append(el('b', { text: int(value) }), document.createTextNode(' ' + name));
  return node;
}

/* --- The rail ----------------------------------------------------------- */

const RAIL_FILTERS = {
  all: () => true,
  fast: (o) => o.elapsedSec < 30,
  mid: (o) => o.elapsedSec >= 30 && o.elapsedSec <= 60,
  slow: (o) => o.elapsedSec > 60,
  stuck: (o) => o.degraded,
};

function renderRail(s) {
  const all = s.orders || [];
  const filter = RAIL_FILTERS[railFilter] || RAIL_FILTERS.all;

  for (const chip of document.querySelectorAll('.chip[data-filter]')) {
    chip.setAttribute('aria-pressed', String(chip.dataset.filter === railFilter));
  }

  // The count comes back from the draw: seating, retiring and filtering all
  // happen there, so counting beforehand would claim more than is on screen.
  const shown = drawTickets(all, s.pipelines || {}, filter);

  $('tickets-count').textContent = all.length
    ? `${shown} on the rail · ${int(s.totals.running)} in flight`
    : '';

  const stuck = s.totals.degraded || 0;
  $('rail-foot').replaceChildren(
    el('span', { class: 'tag', text: `${int(s.totals.completed)} served` }),
    el('span', { class: 'tag', text: `${int(s.totals.running)} in flight` }),
    el('span', {
      class: 'tag',
      text: stuck ? `${int(stuck)} stuck — rescue them from the version card` : 'zero stuck orders',
    }),
  );
}

// leaveMs is how long a finished order takes to fade out. Must match the
// .ticket-leaving animation, or the node is removed mid-fade.
const leaveMs = 550;

// The rail seats each order in a slot it keeps for its whole life.
//
// Ordering by age did not work: elapsedSec is whole seconds, so dozens of
// orders tie, and among ties the sort falls back to the order the backend
// listed them in — which churns as the window slides. Tickets shuffled every
// second even though the sort was doing what it was asked.
//
// Slots remove ordering from the equation. An order is seated once, stays in
// that position until it finishes, fades in place, and only then frees the
// slot for the next arrival. Nothing else moves.
const slots = [];              // slot index -> order ID, or null when free
const seatedNodes = new Map(); // order ID -> its element

// retired holds orders that have finished and faded out.
//
// The backend keeps returning a completed order until it ages out of the
// sampled window, so without this the ticket would be reseated on the very
// next frame after fading away.
const retired = new Set();

// drawnOnce guards the first frame. The window already contains orders that
// finished before the page opened, and animating a screenful of them fading
// at once is just noise — they are retired silently instead.
let drawnOnce = false;

// drawTickets seats arrivals, updates what is seated, and clears what has gone.
//
// Returns how many tickets are on screen.
function drawTickets(orders, pipelines, filter) {
  const container = $('tickets');
  const byId = new Map(orders.map((o) => [o.orderId, o]));

  // Orders that finished before this page opened never appear.
  if (!drawnOnce) {
    for (const order of orders) {
      if (order.done) retired.add(order.orderId);
    }
  }

  // Free any slot whose order has left the window or finished fading.
  for (let i = 0; i < slots.length; i += 1) {
    const id = slots[i];
    if (!id) continue;
    if (!byId.has(id) || retired.has(id)) {
      const node = seatedNodes.get(id);
      if (node) node.remove();
      seatedNodes.delete(id);
      slots[i] = null;
    }
  }

  // Seat arrivals in order ID, which is monotonic with start time and — unlike
  // age — never changes for a given order. Lowest free slot first, so the rail
  // fills its gaps rather than growing.
  const seated = new Set(slots.filter(Boolean));
  const arrivals = orders
    .filter((o) => !seated.has(o.orderId) && !retired.has(o.orderId))
    .sort((a, b) => (a.orderId < b.orderId ? -1 : 1));

  for (const order of arrivals) {
    let index = slots.indexOf(null);
    if (index === -1) index = slots.push(null) - 1;
    slots[index] = order.orderId;
  }

  // Forget retired orders once they leave the window, so the set cannot grow.
  for (const id of retired) {
    if (!byId.has(id)) retired.delete(id);
  }

  // Trim trailing free slots, so the rail is as long as the work in it. Gaps
  // *between* seated orders stay, because those hold position for the ticket
  // after them; trailing ones hold nothing and would just be rows of empty
  // boxes when the rail is far below its cap.
  while (slots.length && slots[slots.length - 1] === null) slots.pop();

  const showing = [];
  const children = [];

  for (const id of slots) {
    const order = id ? byId.get(id) : null;
    if (!order) {
      // An empty slot still holds its place, or every ticket after it would
      // shift the moment one order finished.
      children.push(el('div', { class: 'ticket-slot' }));
      continue;
    }

    let node = seatedNodes.get(id);
    if (!node) {
      node = ticket(order, pipelines);
      seatedNodes.set(id, node);
    }
    updateTicket(node, order, pipelines);

    // A finished order plays out and goes, keeping its slot while it fades.
    if (order.done && !node.dataset.leaving) {
      node.dataset.leaving = '1';
      node.classList.add('ticket-leaving');
      setTimeout(() => retired.add(id), leaveMs);
    }

    if (filter(order)) {
      children.push(node);
      showing.push(id);
    } else {
      // Filtering is a deliberate inspection, so it collapses rather than
      // leaving the rail full of holes. Positions are stable in the default
      // view, which is the one being watched.
      children.push(el('div', { class: 'ticket-slot ticket-slot-hidden' }));
    }
  }

  container.replaceChildren(...children);
  drawnOnce = true;
  return showing.length;
}

function ticket(order, pipelines) {
  const node = el('div', { class: 'ticket' });
  node.dataset.orderId = order.orderId;
  node.style.setProperty('--version-color', colorFor(order.version));

  const top = el('div', { class: 'ticket-top' });
  top.append(
    el('span', { class: 'ticket-id', text: order.orderId.replace(/^ord-0*/, '#') }),
    el('span', { class: 'ticket-version', text: order.version || '…' }),
  );
  node.append(
    top,
    el('div', { class: 'ticket-steps' }),
    el('div', { class: 'ticket-step' }),
    el('div', { class: 'ticket-age' }),
  );

  return updateTicket(node, order, pipelines);
}

// updateTicket writes an order's current state into an existing ticket.
//
// Only the step dots are rebuilt, and only when the order has actually moved:
// redrawing them every tick would restart the pulse on the live dot, which is
// the one thing that shows the order is alive.
function updateTicket(node, order, pipelines) {
  const done = order.done;
  node.className = 'ticket' + (order.degraded ? ' ticket-stuck' : '') + (done ? ' ticket-done' : '');

  const [top, steps, step, ageLine] = node.children;

  // The version has to be refreshed, not just set at creation.
  //
  // An order that has been started but whose first Workflow Task has not run
  // yet has no version: the Workflow publishes that about itself on its first
  // task, so for a moment visibility reports it as Running with no version and
  // no step. A ticket seated in that window used to keep its placeholder and
  // its grey forever, because only the step dots were being updated.
  const shown = order.version || '';
  if (node.dataset.shownVersion !== shown) {
    node.dataset.shownVersion = shown;
    top.children[1].textContent = shown || '…';
    node.style.setProperty('--version-color', colorFor(order.version));
  }

  const reached = done ? 'done' : String(order.step);
  if (steps.dataset.at !== reached) {
    steps.dataset.at = reached;
    steps.replaceChildren(...stepDots(order, pipelines, done));
  }

  // No step yet means the order is queued and no worker has taken it — which
  // is worth saying, rather than showing a dash.
  step.textContent = order.degraded ? 'stuck: ' + order.step
    : order.step || 'waiting for a worker';
  ageLine.textContent = done ? 'served in ' + age(order.elapsedSec) : age(order.elapsedSec);
  return node;
}

// stepDots draws the order's journey as one dot per step of its version's
// pipeline, filled up to where it has got to.
//
// The step index is derived by finding the reported step in that version's
// pipeline, so a v1 order shows four rungs and a v4 order seven — the shape
// difference between versions, on every single order.
function stepDots(order, pipelines, done) {
  const steps = pipelines[order.version] || [];
  if (!steps.length) return [];

  const at = done ? steps.length : Math.max(0, steps.indexOf(order.step));

  const nodes = [];
  steps.forEach((step, i) => {
    if (i > 0) {
      nodes.push(el('span', { class: 'step-link' + (i <= at ? ' step-link-done' : '') }));
    }
    let cls = 'step-dot';
    if (i < at || done) cls += ' step-dot-done';
    else if (i === at) cls += ' step-dot-live';
    const dot = el('span', { class: cls });
    dot.title = step;
    nodes.push(dot);
  });
  return nodes;
}

// renderFault keeps the fault selects in step with the versions that actually
// exist, without stamping over a selection being made.
let faultVersions = '';

function renderFault(s) {
  const labels = (s.deployment.versions || []).map((v) => v.label);
  const key = labels.join(',');

  if (key !== faultVersions) {
    faultVersions = key;
    const select = $('fault-version');
    const keep = select.value;
    select.replaceChildren(...labels.map((label) => el('option', { value: label, text: label })));
    select.value = labels.includes(keep) ? keep : (labels.at(-1) || '');
    renderFaultSteps(s);
  }

  const chaos = s.traffic && s.traffic.chaos;
  const active = chaos && chaos.pct > 0 && chaos.spec;
  $('fault-active').hidden = !active;
  if (active) {
    $('fault-active').textContent =
      `${chaos.spec.targetVersion}: ${chaos.spec.step} ${chaos.spec.mode === 'slow' ? 'runs very slowly' : 'fails'} on ${pct(chaos.pct)} of orders`;
  }
}

function renderFaultSteps(s) {
  const version = $('fault-version').value;
  const steps = (s.pipelines || {})[version] || [];
  const select = $('fault-step');
  const keep = select.value;
  select.replaceChildren(...steps.map((step) => el('option', { value: step, text: step })));
  select.value = steps.includes(keep) ? keep : (steps[1] || steps[0] || '');
}

/* --- Element helpers ---------------------------------------------------- */

function el(tag, options) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(options || {})) {
    if (key === 'text') node.textContent = value;
    else if (key === 'class') node.className = value;
    else node.setAttribute(key, value);
  }
  return node;
}

function svgEl(tag, attrs) {
  const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  return node;
}

function button(label, className, onClick) {
  const node = el('button', { class: ('btn ' + (className || '')).trim(), text: label });
  node.addEventListener('click', onClick);
  return node;
}

/* --- Wiring ------------------------------------------------------------- */

function setRate(rate) {
  act('/api/traffic/rate', { ratePerMin: rate },
    () => (rate > 0 ? `Taking ${int(rate)} orders a minute` : 'Stopped taking new orders'));
}

$('rate-apply').addEventListener('click', () => setRate(Number($('rate-input').value)));

for (const chip of document.querySelectorAll('.chip[data-rate]')) {
  chip.addEventListener('click', () => {
    const rate = Number(chip.dataset.rate);
    $('rate-input').value = rate;
    setRate(rate);
  });
}

for (const chip of document.querySelectorAll('.chip[data-spike]')) {
  chip.addEventListener('click', () => {
    const count = Number(chip.dataset.spike);
    act('/api/traffic/spike', { count }, () => `Dumped ${int(count)} orders`);
  });
}

for (const chip of document.querySelectorAll('.chip[data-filter]')) {
  chip.addEventListener('click', () => {
    railFilter = chip.dataset.filter;
    if (snapshot) renderRail(snapshot);
  });
}

$('fault-version').addEventListener('change', () => {
  if (snapshot) renderFaultSteps(snapshot);
});

$('fault-apply').addEventListener('click', () => {
  act('/api/traffic/chaos', {
    version: $('fault-version').value,
    step: $('fault-step').value,
    mode: $('fault-mode').value,
    pct: Number($('fault-pct').value),
  }, (state) => `${state.chaos.spec.targetVersion} will now fail at ${state.chaos.spec.step}`);
});

$('fault-clear').addEventListener('click', () => {
  act('/api/traffic/chaos', { pct: 0 }, () => 'Fault cleared');
});

// Draw once from the cached snapshot so the page is never blank, then stream.
fetch(url('/api/state'))
  .then((r) => r.json())
  .then(render)
  .catch(() => link('lost', 'backend unreachable'))
  .finally(connect);
