#!/usr/bin/env bash
# Check the dashboard for the mistakes `node --check` cannot see.
#
# node --check validates syntax only. It happily accepts a call to a function
# that does not exist, which is exactly what broke the version cards, the order
# rail and the fault controls in one go — a refactor deleted a function that was
# still being called, and the resulting ReferenceError emptied every panel drawn
# after it.
set -euo pipefail

cd "$(dirname "$0")/../.."

node --check frontend/app.js
echo "syntax OK"

python3 - <<'PY'
import re, sys

js = open('frontend/app.js').read()
html = open('frontend/index.html').read()

failures = []

# 1. Every function called must be defined, or be a parameter holding one.
defined = set(re.findall(r'function\s+([A-Za-z_$][\w$]*)\s*\(', js))
defined |= set(re.findall(r'const\s+([A-Za-z_$][\w$]*)\s*=\s*\(', js))

# Parameters count as defined: a predicate or callback passed in is called by
# name, and is not something this file declares.
params = set()
for plist in re.findall(r'function\s*[A-Za-z_$\w]*\s*\(([^)]*)\)', js):
    params |= {p.strip().split('=')[0].strip() for p in plist.split(',') if p.strip()}
for plist in re.findall(r'\(([^()]*)\)\s*=>', js):
    params |= {p.strip().split('=')[0].strip() for p in plist.split(',') if p.strip()}
params |= set(re.findall(r'([A-Za-z_$][\w$]*)\s*=>', js))

# Destructured bindings count too, e.g. `for (const [name, draw] of ...)`
# binds draw to a function that is then called by name.
for names in re.findall(r'(?:const|let|var)\s*\[([^\]]*)\]', js):
    params |= {n.strip() for n in names.split(',') if n.strip()}
defined |= {p for p in params if re.fullmatch(r'[A-Za-z_$][\w$]*', p or '')}

# A leading dot means a method call on something else, which is not this
# file's business — except that the spread operator looks identical to the
# regex. `...stepDots(x)` is a plain call, so spreads are removed first;
# without this, every call made inside a spread went unchecked.
calls_src = js.replace('...', ' ')
called = set(re.findall(r'(?<![.\w$])([a-z_$][\w$]*)\s*\(', calls_src))

builtins = {
    'if', 'for', 'while', 'switch', 'catch', 'return', 'function', 'typeof', 'new', 'await',
    'else', 'do', 'try', 'throw', 'void', 'isNaN',
    'Number', 'String', 'Math', 'Set', 'Map', 'Date', 'Boolean', 'JSON', 'Array', 'Object',
    'fetch', 'setTimeout', 'parseInt', 'parseFloat', 'console', 'document',
}
# CSS function names that appear inside style strings, not calls.
css = {'mix', 'translateX', 'var'}

undefined = sorted(called - defined - builtins - css)
if undefined:
    failures.append(f'calls to undefined functions: {undefined}')

# 2. Every element the renderer reaches for must exist in the markup.
ids = set(re.findall(r"\$\('([^']+)'\)", js))
present = set(re.findall(r'id="([^"]+)"', html))
missing = sorted(ids - present)
if missing:
    failures.append(f'element ids referenced but not in the markup: {missing}')

# 3. Every endpoint the UI posts to must be routed by the backend.
routes = set(re.findall(r'"(?:GET|POST) (/api/[^"]+)"', open('internal/api/server.go').read()))
calls = sorted(set(re.findall(r"act\('(/api/[^']+)'", js)))
unrouted = [c for c in calls if c not in routes]
if unrouted:
    failures.append(f'endpoints called but not routed: {unrouted}')

if failures:
    for f in failures:
        print('FAIL:', f, file=sys.stderr)
    sys.exit(1)

print(f'{len(defined)} functions, {len(ids)} element ids, {len(calls)} endpoints — all resolve')
PY
