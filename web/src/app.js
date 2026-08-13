const state = { ir: null, revision: null, run: null, stream: null };
const $ = (id) => document.getElementById(id);
const apiBase = () => $('api-base').value.replace(/\/$/, '');
const project = () => $('project-id').value.trim();
const workflow = () => $('workflow-id').value.trim();
const auth = () => ({ Authorization: `Bearer ${$('token').value.trim()}` });

async function api(path, options = {}) {
  const response = await fetch(`${apiBase()}${path}`, { ...options, headers: { ...auth(), 'Content-Type': 'application/json', ...(options.headers || {}) } });
  if (!response.ok) { const body = await response.json().catch(() => ({})); throw body.error || { code: 'HTTP_ERROR', message: response.statusText }; }
  return response.json();
}

function parseIR() { state.ir = JSON.parse($('ir').value); return state.ir; }
function displayDiagnostics(value) { $('diagnostics').textContent = value ? JSON.stringify(value, null, 2) : ''; }
function syncText() { $('ir').value = JSON.stringify(state.ir, null, 2); }

function draw() {
  const nodes = $('nodes'); const edges = $('edges'); nodes.replaceChildren(); edges.replaceChildren();
  if (!state.ir) return;
  const layout = state.ir.layout || {};
  for (const edge of state.ir.edges || []) {
    const source = layout[edge.source] || { x: 0, y: 0 }; const target = layout[edge.target] || { x: 0, y: 0 };
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line'); line.classList.add('edge');
    if (state.run?.failure_location?.logical_edge_id === edge.id) line.classList.add('edge-error');
    line.setAttribute('x1', source.x + 170); line.setAttribute('y1', source.y + 36); line.setAttribute('x2', target.x); line.setAttribute('y2', target.y + 36); edges.append(line);
  }
  for (const node of state.ir.nodes || []) {
    const position = layout[node.id] || { x: 32, y: 32 }; const element = document.createElement('article'); element.className = 'node'; element.dataset.nodeId = node.id; element.style.left = `${position.x}px`; element.style.top = `${position.y}px`;
    const location = state.run?.failure_location?.logical_node_id === node.id ? state.run.failure_location : state.run?.nodes?.find((candidate) => candidate.location?.logical_node_id === node.id)?.location;
    const field = location?.ir_field ? `<small class="field-error" data-error-field="${escapeHTML(location.ir_field)}">${escapeHTML(location.ir_field)}</small>` : '';
    element.innerHTML = `<strong>${escapeHTML(node.title || node.id)}</strong><small>${escapeHTML(node.type)}</small>${field}`;
    element.dataset.error = String(Boolean(location));
    makeDraggable(element, node); nodes.append(element);
  }
}

function makeDraggable(element, node) {
  element.addEventListener('pointerdown', (event) => { const startX = event.clientX; const startY = event.clientY; const origin = state.ir.layout[node.id] || { x: 0, y: 0 }; element.setPointerCapture(event.pointerId); const move = (next) => { state.ir.layout[node.id] = { x: Math.max(0, origin.x + next.clientX - startX), y: Math.max(0, origin.y + next.clientY - startY) }; syncText(); draw(); }; element.addEventListener('pointermove', move); element.addEventListener('pointerup', () => element.removeEventListener('pointermove', move), { once: true }); });
}

async function pull() { const draft = await api(`/v1/projects/${project()}/workflows/${workflow()}/draft`, { method: 'GET', headers: auth() }); state.ir = draft.current.ir; state.revision = draft.current.revision_number; syncText(); draw(); }
async function save() { const ir = parseIR(); const saved = await api(`/v1/projects/${project()}/workflows/${workflow()}/draft`, { method: 'PUT', headers: { ...auth(), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ expected_revision: state.revision, ir }) }); state.revision = saved.revision_number; state.ir = saved.ir; syncText(); draw(); }
async function validate() { const result = await api(`/v1/projects/${project()}/workflows/${workflow()}/draft/validate`, { method: 'POST', body: JSON.stringify({ revision: state.revision }) }); displayDiagnostics(result.diagnostics); }
async function test() { const deadline = new Date(Date.now() + 5 * 60_000).toISOString(); const run = await api(`/v1/projects/${project()}/workflows/${workflow()}/draft/test`, { method: 'POST', headers: { ...auth(), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ revision: state.revision, input: {}, deadline_at: deadline }) }); await loadRun(run.run_id); }
async function publish() { const result = await api(`/v1/projects/${project()}/workflows/${workflow()}/publish`, { method: 'POST', headers: { ...auth(), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ expected_revision: state.revision, change_log: 'Published from Human Web Canvas' }) }); displayDiagnostics(`Published version ${result.version.version_number}`); }
async function run() { const deadline = new Date(Date.now() + 5 * 60_000).toISOString(); const result = await api(`/v1/projects/${project()}/workflows/${workflow()}/runs`, { method: 'POST', headers: { ...auth(), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ input: {}, deadline_at: deadline }) }); await loadRun(result.run_id); }
async function cancel() { if (!state.run?.run_id) throw new Error('请先选择一个运行实例'); const result = await api(`/v1/projects/${project()}/runs/${state.run.run_id}/cancel`, { method: 'POST', headers: auth() }); displayDiagnostics(result); await loadRun(state.run.run_id); }
async function loadRun(runID) { state.run = await api(`/v1/projects/${project()}/runs/${runID}`, { method: 'GET', headers: auth() }); $('run-state').textContent = `${state.run.state} (v${state.run.state_version})`; $('run-error').textContent = JSON.stringify(state.run.failure_location || {}, null, 2); draw(); subscribe(runID); }
function subscribe(runID) {
  state.stream?.abort();
  const controller = new AbortController(); state.stream = controller;
  readRunEvents(runID, controller.signal).catch(() => {
    if (!controller.signal.aborted) setTimeout(() => loadRun(runID).catch(() => {}), 1500);
  });
}
async function readRunEvents(runID, signal) {
  const response = await fetch(`${apiBase()}/v1/projects/${project()}/runs/${runID}/events`, { headers: { ...auth(), Accept: 'text/event-stream' }, signal });
  if (!response.ok || !response.body) throw new Error('run event stream is unavailable');
  const reader = response.body.getReader(); const decoder = new TextDecoder(); let buffer = '';
  while (!signal.aborted) {
    const { value, done } = await reader.read(); if (done) break; buffer += decoder.decode(value, { stream: true });
    const frames = buffer.split('\n\n'); buffer = frames.pop();
    for (const frame of frames) if (frame.includes('event: update')) await loadRun(runID);
  }
}
function escapeHTML(value) { return String(value).replace(/[&<>"']/g, (char) => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#039;' })[char]); }

for (const [id, action] of Object.entries({ load: refreshAuthoringContext, save, validate, test, publish, run, cancel })) $(id).addEventListener('click', () => action().catch(displayDiagnostics));
function refreshAuthoringContext() {
  if (!project() || !workflow() || !$('token').value.trim() || !apiBase()) return Promise.resolve();
  return pull().then(loadCatalog);
}
for (const id of ['workflow-id', 'project-id', 'token', 'api-base']) $(id).addEventListener('change', () => refreshAuthoringContext().catch(displayDiagnostics));
function addCatalogNode(description) {
  if (!state.ir || !description.examples?.length) return;
  const node = structuredClone(description.examples[0]);
  const suffix = (state.ir.nodes || []).filter((item) => item.type === node.type).length + 1;
  node.id = `${node.type.replaceAll('.', '_')}_${suffix}`;
  node.title = `${node.title || node.type} ${suffix}`;
  state.ir.nodes.push(node);
  state.ir.layout ||= {};
  state.ir.layout[node.id] = { x: 96 + suffix * 28, y: 96 + suffix * 28 };
  syncText(); draw();
}
async function loadCatalog() { const { node_types } = await api('/v1/node-types', { method: 'GET', headers: auth() }); $('catalog').replaceChildren(...node_types.map((node) => { const button = document.createElement('button'); button.className = 'catalog-item'; button.textContent = `${node.type} · ${node.kind}`; button.title = node.natural_language_description; button.addEventListener('click', () => addCatalogNode(node)); return button; })); }
