import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

test('canvas only calls External API and has no DSL upload surface', async () => {
  const source = await readFile('src/app.js', 'utf8');
  assert.match(source, /\/draft\/test/);
  assert.match(source, /\/runs\//);
  assert.match(source, /async function run/);
  assert.match(source, /async function cancel/);
  assert.match(source, /Authorization/);
  assert.doesNotMatch(source, /new EventSource/);
  assert.doesNotMatch(source, /\/dsl(?:["'`/]|$)/i);
  assert.doesNotMatch(source, /source_map/i);
});

test('canvas keeps layout in authoring IR', async () => {
  const source = await readFile('src/app.js', 'utf8');
  assert.match(source, /state\.ir\.layout\[node\.id\]/);
  assert.match(source, /syncText\(\); draw\(\)/);
  assert.match(source, /addCatalogNode/);
  assert.match(source, /edge-error/);
  assert.match(source, /data-error-field/);
  assert.match(source, /refreshAuthoringContext/);
  assert.match(source, /load: refreshAuthoringContext/);
});
