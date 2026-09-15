/* Order ops console.
 *
 * A pure renderer: the backend sends a complete snapshot every second over
 * Server-Sent Events, and this file draws it. There is no client-side model of
 * the world to drift out of step with the server, which matters when the thing
 * on screen is being used to make deploy decisions in front of an audience. */

'use strict';

// The API is same-origin in the containerised setup, where nginx proxies /api
// and /events to the backend. ?api=http://host:port points a dev build at a
// backend running elsewhere.
const API = new URLSearchParams(location.search).get('api') || '';
const url = (path) => API + path;

const VERSION_COLOR = {
  v1: 'var(--v1)', v2: 'var(--v2)', v3: 'var(--v3)',
  v4: 'var(--v4)', v5: 'var(--v5)',
};
const colorFor = (label) => VERSION_COLOR[label] || 'var(--ink-faint)';

const $ = (id) => document.getElementById(id);

let snapshot = null;

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

function render(next) {
  snapshot = next;
  renderReadouts(next);
  renderSplit(next);
  renderCapacity(next);
  renderRollout(next);
  renderStations(next);
  renderTickets(next);
  renderFault(next);
}

function renderReadouts(s) {
  const traffic = s.traffic;
  $('rate-actual').textContent = traffic && traffic.running ? int(traffic.ratePerMin) : '0';
  renderAutoStop(traffic);
  $('throughput').textContent = int(s.completedPerMin);
  $('inflight').textContent = int(s.totals.running);

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

// renderAutoStop says when traffic will stop itself.
//
// Worth showing rather than hiding: with serverless workers, traffic left
// running keeps invoking Lambdas, so the generator stops on its own — and an
// operator who does not know that would think the demo broke.
function renderAutoStop(traffic) {
  const note = $('autostop');
  if (!traffic) {
    note.hidden = true;
    return;
  }

  if (traffic.autoStopped) {
    note.hidden = false;
    note.textContent = 'Traffic stopped itself after being left untouched. Set a rate to start again.';
    return;
  }

  const stopAt = traffic.stopAt ? new Date(traffic.stopAt) : null;
  if (!stopAt || !traffic.ratePerMin || !traffic.running) {
    note.hidden = true;
    return;
  }

  const seconds = Math.round((stopAt - Date.now()) / 1000);
  note.hidden = seconds > 300; // only worth saying when it is close
  if (!note.hidden) note.textContent = `Traffic stops itself in ${age(seconds)} unless you change something.`;
}

// renderSplit draws the two hero bars: where new orders are routed, and where
// orders actually are. The gap between them is the pinning guarantee.
function renderSplit(s) {
  const routing = s.deployment.routing || {};
  const shares = [];

  if (routing.currentLabel) {
    shares.push({ label: routing.currentLabel, share: 100 - (routing.rampingPct || 0) });
  }
  if (routing.rampingLabel && routing.rampingPct > 0) {
    shares.push({ label: routing.rampingLabel, share: routing.rampingPct });
  }
  drawBar($('split-routing'), shares, 'No version is taking orders yet', (s) => pct(s.share));

  const inFlight = (s.deployment.versions || [])
    .map((v) => ({ label: v.label, share: (s.health[v.label] || {}).running || 0 }))
    .filter((v) => v.share > 0);
  drawBar($('split-inflight'), inFlight, 'No orders in flight', (s) => int(s.share));

  // Where the workers actually are. With serverless workers this bar is empty
  // at rest and fills per version as work arrives, so dumping orders onto an
  // idle version visibly brings its own slice into existence.
  const workers = (s.deployment.versions || [])
    .map((v) => ({ label: v.label, share: v.pollers || 0 }))
    .filter((v) => v.share > 0);
  drawBar($('split-workers'), workers, 'No workers running anywhere — nothing is provisioned until there is work',
    (s) => int(s.share));
}

function drawBar(target, shares, emptyMessage, caption) {
  const total = shares.reduce((sum, s) => sum + s.share, 0);
  if (total <= 0) {
    target.replaceChildren(el('span', { class: 'split-empty', text: emptyMessage }));
    return;
  }

  target.replaceChildren(...shares.map((share) => {
    const segment = el('div', { class: 'split-seg' });
    segment.style.background = colorFor(share.label);
    segment.style.flex = `${share.share} 1 0`;
    segment.append(
      el('span', { class: 'split-seg-name', text: share.label }),
      el('span', { text: caption(share) }),
    );
    return segment;
  }));
}

function renderCapacity(s) {
  const capacity = s.capacity || {};
  $('backlog').textContent = int(capacity.backlogDepth);
  $('pollers').textContent = int(capacity.pollers);

  // Zero wait is the good case and deserves to read as such, rather than as
  // the number 0.
  const wait = capacity.oldestWaitSec || 0;
  $('oldest-wait').textContent = wait < 1 ? 'none' : age(wait);

  // Describe what the queue is doing, not whether the workers are "keeping
  // up". Arrival and pickup rates match exactly when a saturated queue holds
  // steady, so a rate comparison alone would report everything as fine while
  // orders sit waiting for minutes.
  const added = capacity.addedPerSec || 0;
  const taken = capacity.dispatchedPerSec || 0;
  const queued = capacity.backlogDepth || 0;

  let note = 'When work arrives faster than workers can take it, the queue grows and more workers are started.';
  if (added > 0 || taken > 0 || queued > 0) {
    const state = added > taken * 1.05 ? 'the backlog is growing'
      : taken > added * 1.05 ? 'the backlog is draining'
      : queued > 0 ? `the backlog is holding at ${int(queued)}`
      : 'nothing is waiting for a worker';
    note = `${added.toFixed(1)} tasks a second arriving, ${taken.toFixed(1)} picked up — ${state}.`;
  }
  $('capacity-note').textContent = note;

  renderSyncMatch(s.syncMatch || {});

  const history = s.history || {};
  spark($('spark-backlog'), history.backlog, 'var(--v5)');
  spark($('spark-pollers'), history.pollers, 'var(--v1)');
  spark($('spark-wait'), history.oldestWaitSec, 'var(--v3)');
  spark($('spark-syncmatch'), history.syncMatchPct, 'var(--v2)', 100);
}

// renderSyncMatch shows the real sync match rate: the share of tasks the
// server handed straight to a worker that was already waiting, rather than
// writing to the backlog first. This is the signal Temporal scales on.
//
// Unlike the other gauges it can be genuinely unknown — it comes from the
// server's metrics endpoint, which may not be reachable — and saying so is
// better than showing a confident zero.
function renderSyncMatch(syncMatch) {
  const gauge = $('syncmatch').closest('.gauge');
  const known = syncMatch.ratePct >= 0;

  gauge.classList.toggle('gauge-unavailable', !known);
  $('syncmatch').textContent = known ? pct(syncMatch.ratePct) : '—';

  if (!known) {
    $('syncmatch-name').textContent = syncMatch.available
      ? 'handed straight to a waiting worker — no tasks yet'
      : 'handed straight to a waiting worker — server metrics unavailable';
    return;
  }

  // Say how many tasks the figure is based on: 100% of four tasks and 100% of
  // four hundred are not the same claim.
  $('syncmatch-name').textContent = syncMatch.delivered > 0
    ? `handed straight to a waiting worker, of ${int(syncMatch.delivered)} just now`
    : 'handed straight to a waiting worker';
}

// spark draws a filled sparkline. A fixed max is passed for series that are
// percentages, so 100% always looks like a full-height line.
function spark(svg, series, color, fixedMax) {
  if (!series || series.length < 2) {
    svg.replaceChildren();
    return;
  }

  const width = 120, height = 28;
  const max = Math.max(fixedMax || 0, ...series, 1);
  const points = series.map((value, i) => {
    const x = (i / (series.length - 1)) * width;
    const y = height - (Math.max(0, value) / max) * (height - 2) - 1;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const area = svgEl('polygon', {
    class: 'spark-area',
    points: `0,${height} ${points.join(' ')} ${width},${height}`,
  });
  const line = svgEl('polyline', { class: 'spark-line', points: points.join(' ') });

  area.style.fill = `color-mix(in srgb, ${color} 18%, transparent)`;
  line.style.stroke = color;
  svg.replaceChildren(area, line);
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
  const rollout = s.rollout;

  if (!rollout || !rollout.targetVersion) {
    body.replaceChildren(el('p', {
      class: 'empty',
      text: 'No deployment running. Pick a version below and start one.',
    }));
    return;
  }

  const live = LIVE_PHASES.has(rollout.phase);
  const parts = [];

  const head = el('div', { class: 'rollout-head' });
  const target = el('span', { class: 'rollout-target', text: rollout.targetVersion });
  target.style.color = colorFor(rollout.targetVersion);
  head.append(
    target,
    el('span', {
      class: 'rollout-phase phase-' + rollout.phase.toLowerCase(),
      text: PHASE_TEXT[rollout.phase] || rollout.phase,
    }),
  );
  if (rollout.previousCurrentLabel) {
    head.append(el('span', { class: 'rollout-stat-name', text: 'from ' + rollout.previousCurrentLabel }));
  }
  parts.push(head);

  parts.push(renderStages(rollout));

  if (rollout.message) {
    parts.push(el('p', {
      class: 'rollout-message' + (BAD_PHASES.has(rollout.phase) ? ' rollout-message-bad' : ''),
      text: rollout.message,
    }));
  }

  parts.push(renderRolloutStats(rollout));

  if (live) parts.push(renderRolloutActions(rollout));

  if (rollout.manualControl) {
    parts.push(el('p', {
      class: 'manual-flag',
      text: 'You set this share by hand. It will hold here, still watching for trouble, until you continue or stop it.',
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
  // "50% holding" while the ramp — and every other number on the panel — was
  // at 25%.
  const current = rollout.currentPct;
  const onAStage = stages.some((stage) => stage.pct === current);

  stages.forEach((stage) => {
    let state = '';
    if (stage.pct < current) state = 'stage-done';
    else if (stage.pct === current && live) state = 'stage-live';
    else if (stage.pct === current) state = 'stage-done';

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

  // An operator can set any share, not only one the plan lists. Say so rather
  // than silently highlighting nothing.
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
      health.samples ? `failed or stuck, of ${int(health.samples)} orders` : 'no orders judged yet',
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
  const node = el('div', { class: 'rollout-stat' + (bad ? ' rollout-stat-bad' : '') });
  node.append(
    el('span', { class: 'rollout-stat-value', text: value }),
    el('span', { class: 'rollout-stat-name', text: name }),
  );
  return node;
}

function renderRolloutActions(rollout) {
  const row = el('div', { class: 'rollout-actions' });

  if (rollout.phase === 'paused') {
    row.append(button('Continue', 'btn-primary', () =>
      act('/api/rollout/resume', undefined, () => 'Deployment continuing')));
  } else {
    row.append(button('Pause', '', () =>
      act('/api/rollout/pause', undefined, () => 'Deployment paused')));
  }

  row.append(button('Next stage', '', () =>
    act('/api/rollout/advance', undefined, () => 'Moved to the next stage')));

  const jump = el('div', { class: 'rollout-jump' });
  const input = el('input', { type: 'number', min: '0', max: '100', step: '5' });
  input.value = Math.round(rollout.currentPct);
  input.setAttribute('aria-label', 'Share of new orders');
  jump.append(input, button('Set share', '', () =>
    act('/api/rollout/ramp', { pct: Number(input.value) },
      (state) => `Sending ${pct(state.currentPct)} of new orders to ${state.targetVersion}`)));
  row.append(jump);

  row.append(button('Stop and roll back', 'btn-danger', () =>
    act('/api/rollout/abort', { rollback: true }, () => 'Rolled back')));

  return row;
}

function renderStations(s) {
  const routing = s.deployment.routing || {};
  const versions = s.deployment.versions || [];
  const rolloutLive = s.rollout && LIVE_PHASES.has(s.rollout.phase);

  // Steps the current version already has: anything else a candidate runs is
  // a change worth pointing at.
  const currentSteps = new Set((s.pipelines || {})[routing.currentLabel] || []);

  $('stations').replaceChildren(...versions.map((version) => {
    const health = s.health[version.label] || {};
    const stuck = health.degraded || 0;

    const station = el('div', { class: 'station' + (stuck > 0 ? ' station-trouble' : '') });
    station.style.setProperty('--version-color', colorFor(version.label));

    const head = el('div', { class: 'station-head' });
    const nameRow = el('div', { class: 'station-name-row' });
    nameRow.append(
      el('span', { class: 'station-name', text: version.label }),
      el('span', { class: 'station-role station-role-' + version.status, text: roleText(version) }),
    );
    head.append(nameRow);

    const share = el('div', { class: 'station-share' });
    share.append(
      el('span', { class: 'station-share-value', text: pct(version.trafficPct) }),
      el('span', { class: 'station-share-name', text: 'of new orders' }),
    );
    head.append(share);

    // Workers first: with serverless workers this is the number that tells
    // the story, and a version at zero is the normal resting state.
    const workers = el('div', { class: 'station-workers' + (version.pollers ? ' station-workers-live' : '') });
    workers.append(
      el('span', { class: 'station-workers-value', text: int(version.pollers || 0) }),
      el('span', {
        class: 'station-workers-name',
        text: version.pollers ? 'workers running' : 'no workers running',
      }),
    );
    head.append(workers);

    const counts = el('div', { class: 'station-counts' });
    counts.append(countNode('in flight', health.running || 0));
    counts.append(countNode('served', health.completed || 0));
    if (stuck > 0) {
      const node = countNode('stuck', stuck);
      node.className = 'station-stuck';
      counts.append(node);
    }
    head.append(counts);
    station.append(head);

    const ladder = el('ul', { class: 'ladder' });
    for (const step of (s.pipelines || {})[version.label] || []) {
      const isNew = version.label !== routing.currentLabel && !currentSteps.has(step);
      ladder.append(el('li', { class: 'rung' + (isNew ? ' rung-new' : ''), text: step }));
    }
    station.append(ladder);

    const foot = el('div', { class: 'station-foot' });
    if (version.label === routing.currentLabel) {
      foot.append(el('span', { class: 'station-role', text: 'Taking orders now' }));
    } else {
      const start = button('Start deployment', 'btn-primary', () =>
        act('/api/rollout', { targetVersion: version.label },
          (state) => `Deploying ${state.targetVersion}: checking it first`));
      start.disabled = rolloutLive;
      if (rolloutLive) start.title = 'A deployment is already running';
      foot.append(start);
    }
    // Offered whenever a version has trouble and is not the one taking
    // orders. It moves everything still running there, not only what is
    // already stuck: the rest is on its way to the same broken step.
    // Dump orders straight onto this version, whatever the routing says.
    // Aimed at idle versions especially: they have no workers running, so the
    // burst makes serverless workers appear from nothing.
    foot.append(dumpControl(version.label));

    if (stuck > 0 && version.label !== routing.currentLabel) {
      const running = health.running || 0;
      const move = button(`Move ${int(running)} orders to ${routing.currentLabel}`, '', () =>
        act('/api/orders/recover', { version: version.label }, (data) => data.message));
      move.title = `Restart every order still running on ${version.label}, pinned to ${routing.currentLabel}`;
      foot.append(move);
    }
    station.append(foot);

    return station;
  }));
}

// dumpControl is the per-version burst control: a count and a button.
function dumpControl(label) {
  const row = el('div', { class: 'dump' });

  const count = el('input', { type: 'number', min: '1', max: '20000', step: '50', class: 'dump-count' });
  count.value = 250;
  count.setAttribute('aria-label', `Orders to send straight to ${label}`);

  const send = button(`Send to ${label}`, '', () =>
    act('/api/versions/dump', { version: label, count: Number(count.value) },
      () => `Sent ${int(count.value)} orders straight to ${label}`));
  send.title = `Start ${label} orders directly, pinned to ${label}, ignoring the traffic split`;

  row.append(count, send);
  return row;
}

function roleText(version) {
  switch (version.status) {
    case 'current': return 'taking new orders';
    case 'ramping': return 'being deployed';
    case 'draining': return 'finishing its orders';
    default: return 'ready, idle';
  }
}

function countNode(name, value) {
  const node = el('span', {});
  node.append(el('b', { text: int(value) }), document.createTextNode(' ' + name));
  return node;
}

function renderTickets(s) {
  const orders = s.orders || [];
  $('tickets-count').textContent = orders.length
    ? `most recent ${orders.length} of ${int(s.totals.running)} in flight`
    : '';

  $('tickets').replaceChildren(...orders.map((order) => {
    const done = order.status === 'Completed';
    const ticket = el('div', {
      class: 'ticket' + (order.degraded ? ' ticket-stuck' : '') + (done ? ' ticket-done' : ''),
    });
    ticket.style.setProperty('--version-color', colorFor(order.version));

    const top = el('div', { class: 'ticket-top' });
    top.append(
      el('span', { class: 'ticket-id', text: order.orderId.replace(/^ord-0*/, '#') }),
      el('span', { class: 'ticket-version', text: order.version || '?' }),
    );
    ticket.append(
      top,
      el('div', { class: 'ticket-step', text: order.degraded ? 'stuck: ' + order.step : order.step || '—' }),
      el('div', { class: 'ticket-age', text: done ? 'served in ' + age(order.elapsedSec) : age(order.elapsedSec) }),
    );
    return ticket;
  }));
}

// renderFault keeps the fault-injection selects in step with the versions that
// actually exist, without stamping over a selection being made.
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

$('rate-apply').addEventListener('click', () => {
  const rate = Number($('rate-input').value);
  act('/api/traffic/rate', { ratePerMin: rate },
    () => (rate > 0 ? `Taking ${int(rate)} orders a minute` : 'Stopped taking new orders'));
});

for (const chip of document.querySelectorAll('.chip[data-rate]')) {
  chip.addEventListener('click', () => {
    const rate = Number(chip.dataset.rate);
    $('rate-input').value = rate;
    act('/api/traffic/rate', { ratePerMin: rate },
      () => (rate > 0 ? `Taking ${int(rate)} orders a minute` : 'Stopped taking new orders'));
  });
}

for (const chip of document.querySelectorAll('.chip[data-spike]')) {
  chip.addEventListener('click', () => {
    const count = Number(chip.dataset.spike);
    act('/api/traffic/spike', { count }, () => `Dumped ${int(count)} orders`);
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
