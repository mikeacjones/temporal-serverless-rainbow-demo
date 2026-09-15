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

function render(next) {
  snapshot = next;
  renderReadouts(next);
  renderSpectrum(next);
  renderLegend(next);
  renderRollout(next);
  renderFleet(next);
  renderStations(next);
  renderRail(next);
  renderFault(next);
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

  const workers = versions
    .map((v) => ({ label: v.label, share: v.pollers || 0 }))
    .filter((v) => v.share > 0);
  drawBar($('split-workers'), workers, 'No workers running anywhere', (x) => int(x.share));

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
  $('pollers').textContent = int(capacity.pollers);
  $('pollers').className = 'gauge-value' + (capacity.pollers ? ' gauge-value-live' : '');
  $('backlog').textContent = int(capacity.backlogDepth);

  // Zero wait is the good case and deserves to read as such.
  const wait = capacity.oldestWaitSec || 0;
  $('oldest-wait').textContent = wait < 1 ? 'none' : age(wait);

  renderSyncMatch(s.syncMatch || {});

  // Say which way the backlog is going, in the plainest terms available.
  // Arrival and pickup rates match exactly when a saturated queue holds
  // steady, so a rate comparison alone would report everything as fine while
  // orders sit waiting for minutes.
  const added = capacity.addedPerSec || 0;
  const taken = capacity.dispatchedPerSec || 0;
  const queued = capacity.backlogDepth || 0;

  let note = 'Nothing is provisioned until there is work. Send orders to a version and its workers appear.';
  if (added > 0 || taken > 0 || queued > 0) {
    const state = added > taken * 1.05 ? 'the backlog is growing'
      : taken > added * 1.05 ? 'the backlog is draining'
      : queued > 0 ? `the backlog is holding at ${int(queued)}`
      : 'nothing is waiting for a worker';
    note = `${added.toFixed(1)} tasks a second arriving, ${taken.toFixed(1)} picked up — ${state}.`;
  }
  $('capacity-note').textContent = note;

  const history = s.history || {};
  spark($('spark-pollers'), history.pollers, 'var(--cyan)');
  spark($('spark-backlog'), history.backlog, 'var(--v4)');

  // Name the peak over the window the graph actually shows, so the figure is
  // scoped rather than implied.
  const peakWorkers = peakOf(history.pollers);
  $('pollers-name').textContent = peakWorkers > (capacity.pollers || 0)
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
function spark(svg, series, color) {
  if (!series || series.length < 2) {
    svg.replaceChildren();
    return;
  }

  const width = 120, height = 30;
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

function renderStations(s) {
  const routing = s.deployment.routing || {};
  const versions = s.deployment.versions || [];
  const rolloutLive = s.rollout && LIVE_PHASES.has(s.rollout.phase);

  $('roster-count').textContent = `${versions.length} registered`;

  $('stations').replaceChildren(...versions.map((version) => {
    const health = s.health[version.label] || {};
    const stuck = health.degraded || 0;
    const serving = version.trafficPct > 0;

    const station = el('div', {
      class: 'station' + (stuck > 0 ? ' station-trouble' : serving ? ' station-active' : ''),
    });
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

    // Workers next: with serverless workers this is the number that tells the
    // story, and a version at zero is the normal resting state.
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
    // The pipeline is no longer listed step by step here — every order ticket
    // draws it as dots — but its length is what differs between versions, so
    // it stays as a number.
    const steps = ((s.pipelines || {})[version.label] || []).length;
    if (steps) counts.append(countNode('steps', steps));
    counts.append(countNode('in flight', health.running || 0));
    counts.append(countNode('served', health.completed || 0));
    if (stuck > 0) {
      const node = countNode('stuck', stuck);
      node.className = 'station-stuck';
      counts.append(node);
    }
    head.append(counts);
    station.append(head);

    const foot = el('div', { class: 'station-foot' });
    if (version.label === routing.currentLabel) {
      foot.append(el('span', { class: 'station-role station-role-current', text: 'Taking orders now' }));
    } else {
      const start = button('Start deployment', 'btn-primary', () =>
        act('/api/rollout', { targetVersion: version.label },
          (state) => `Deploying ${state.targetVersion}: checking it first`));
      start.disabled = rolloutLive;
      if (rolloutLive) start.title = 'A deployment is already running';
      foot.append(start);
    }

    // Send orders straight here, whatever the routing says. Aimed at idle
    // versions especially: they have no workers running, so the burst makes
    // serverless workers appear from nothing.
    foot.append(dumpControl(version.label));

    if (stuck > 0 && version.label !== routing.currentLabel) {
      const running = health.running || 0;
      const move = button(`Move ${int(running)} to ${routing.currentLabel}`, '', () =>
        act('/api/orders/recover', { version: version.label }, (data) => data.message));
      move.title = `Restart every order still running on ${version.label}, pinned to ${routing.currentLabel}`;
      foot.append(move);
    }
    station.append(foot);

    return station;
  }));
}

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
  // Oldest first, and held in that order.
  //
  // Visibility hands these back newest-first, which meant every ticket moved
  // every second and you could not follow one order. Sorted by age the rail
  // becomes a conveyor: an order joins at the end, rises as the ones ahead of
  // it complete and drop off, and finally leaves from the front — so a single
  // order can be watched filling its steps until it disappears.
  const all = (s.orders || []).slice().sort((a, b) => b.elapsedSec - a.elapsedSec);
  const orders = all.filter(RAIL_FILTERS[railFilter] || RAIL_FILTERS.all);

  for (const chip of document.querySelectorAll('.chip[data-filter]')) {
    chip.setAttribute('aria-pressed', String(chip.dataset.filter === railFilter));
  }

  // The count comes back from the draw, because finished orders are retired
  // during it — counting before would claim more tickets than are on screen.
  const shown = drawTickets(orders, s.pipelines || {});

  $('tickets-count').textContent = all.length
    ? `${shown} on the rail · oldest first · ${int(s.totals.running)} in flight`
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

// retired holds orders that have finished and faded out.
//
// The backend keeps returning a completed order until it ages out of the
// sampled window, so without this the ticket would be recreated on the very
// next frame after fading away.
const retired = new Set();

// drawnOnce guards the first frame. The window already contains orders that
// finished before the page opened, and animating a screenful of them fading
// at once is just noise — they are retired silently instead.
let drawnOnce = false;

// drawTickets reconciles the rail by order ID rather than rebuilding it.
//
// Rebuilding every second destroyed and recreated every ticket, which restarts
// the step-dot animation and throws away the DOM identity that makes an
// individual order followable. Reusing the node keeps both: passing existing
// nodes to replaceChildren *moves* them instead of recreating them, so a
// ticket that survives a tick keeps its element and its running animation.
function drawTickets(orders, pipelines) {
  const container = $('tickets');
  const existing = new Map();
  for (const node of container.children) existing.set(node.dataset.orderId, node);

  // Forget orders that have left the window, so the set cannot grow unbounded.
  const present = new Set(orders.map((o) => o.orderId));
  for (const id of retired) {
    if (!present.has(id)) retired.delete(id);
  }

  const nodes = [];
  for (const order of orders) {
    if (retired.has(order.orderId)) continue;

    // Orders that finished before this page opened never appear.
    if (!drawnOnce && order.status === 'Completed') {
      retired.add(order.orderId);
      continue;
    }

    const node = existing.get(order.orderId) || ticket(order, pipelines);
    updateTicket(node, order, pipelines);

    // A finished order plays out and goes. It keeps its place in the rail
    // while it fades, so the orders around it do not jump.
    if (order.status === 'Completed' && !node.dataset.leaving) {
      node.dataset.leaving = '1';
      node.classList.add('ticket-leaving');
      const id = order.orderId;
      setTimeout(() => {
        retired.add(id);
        node.remove();
      }, leaveMs);
    }

    nodes.push(node);
  }

  container.replaceChildren(...nodes);
  drawnOnce = true;
  return nodes.length;
}

function ticket(order, pipelines) {
  const node = el('div', { class: 'ticket' });
  node.dataset.orderId = order.orderId;
  node.style.setProperty('--version-color', colorFor(order.version));

  const top = el('div', { class: 'ticket-top' });
  top.append(
    el('span', { class: 'ticket-id', text: order.orderId.replace(/^ord-0*/, '#') }),
    el('span', { class: 'ticket-version', text: order.version || '?' }),
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
  const done = order.status === 'Completed';
  node.className = 'ticket' + (order.degraded ? ' ticket-stuck' : '') + (done ? ' ticket-done' : '');

  const [, steps, step, ageLine] = node.children;

  const reached = done ? 'done' : String(order.step);
  if (steps.dataset.at !== reached) {
    steps.dataset.at = reached;
    steps.replaceChildren(...stepDots(order, pipelines, done));
  }

  step.textContent = order.degraded ? 'stuck: ' + order.step : order.step || '—';
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
