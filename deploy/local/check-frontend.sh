#!/usr/bin/env bash
# Check the dashboard for the mistakes `node --check` cannot see.
#
# node --check validates syntax only. It accepts a call to a function that does
# not exist and a read of a variable that was never declared — and both of those
# shipped, from edits that replaced a span of the file and silently swallowed
# declarations living inside it. Each one emptied a panel at runtime.
set -euo pipefail

cd "$(dirname "$0")/../.."

node --check frontend/app.js
echo "syntax OK"

python3 - <<'PY'
import re, sys

raw = open('frontend/app.js').read()
html = open('frontend/index.html').read()

# Work on a copy with comments and string bodies removed, so words inside
# prose and messages are not mistaken for code.
# Order matters. Strings come out before line comments, or a URL inside a
# string ("http://…") has its own tail eaten as a comment, leaving a dangling
# quote that breaks every strip after it. Regex literals come out too, or the
# words inside them read as identifiers.
code = re.sub(r'/\*.*?\*/', ' ', raw, flags=re.S)
code = re.sub(r'`(?:[^`\\]|\\.)*`', '``', code)
code = re.sub(r"'(?:[^'\\\n]|\\.)*'", "''", code)
code = re.sub(r'"(?:[^"\\\n]|\\.)*"', '""', code)
code = re.sub(r'(?<=[(,=:\[])\s*/(?:[^/\n\\]|\\.)+/[gimsuy]*', ' //re ', code)
code = re.sub(r'//[^\n]*', ' ', code)

# Object literal keys are not variable reads.
keys = re.sub(r'(?<=[{,])\s*([A-Za-z_$][\w$]*)\s*:', ' ', code)

declared = set()
declared |= set(re.findall(r'function\s+([A-Za-z_$][\w$]*)', code))
declared |= set(re.findall(r'(?:const|let|var)\s+([A-Za-z_$][\w$]*)', code))
for names in re.findall(r'(?:const|let|var)\s*[\[{]([^\]}]*)[\]}]', code):
    declared |= {n.strip().split(':')[-1].strip() for n in names.split(',') if n.strip()}
for plist in re.findall(r'function\s*[A-Za-z_$\w]*\s*\(([^)]*)\)', code):
    declared |= {p.strip().split('=')[0].strip() for p in plist.split(',') if p.strip()}
for plist in re.findall(r'\(([^()]*)\)\s*=>', code):
    declared |= {p.strip().split('=')[0].strip() for p in plist.split(',') if p.strip()}
declared |= set(re.findall(r'([A-Za-z_$][\w$]*)\s*=>', code))
declared |= set(re.findall(r'for\s*\(\s*(?:const|let|var)\s+([A-Za-z_$][\w$]*)', code))
declared |= set(re.findall(r'catch\s*\(\s*([A-Za-z_$][\w$]*)', code))
declared = {d for d in declared if re.fullmatch(r'[A-Za-z_$][\w$]*', d or '')}

keywords = {
    'if','else','for','while','do','switch','case','default','break','continue','return',
    'function','const','let','var','new','typeof','instanceof','in','of','delete','void',
    'try','catch','finally','throw','class','extends','super','this','async','await','yield',
    'true','false','null','undefined','NaN','Infinity','arguments','static','get','set',
}
browser = {
    'window','document','console','fetch','EventSource','URLSearchParams','location','navigator',
    'setTimeout','clearTimeout','setInterval','clearInterval','requestAnimationFrame',
    'JSON','Math','Number','String','Object','Array','Set','Map','Date','Boolean','Promise',
    'isNaN','parseInt','parseFloat','Error','RegExp','Symbol','BigInt',
}

# Identifiers actually read, excluding property access and object keys.
used = set(re.findall(r'(?<![.\w$])([A-Za-z_$][\w$]*)', keys))

failures = []

undeclared = sorted(used - declared - keywords - browser)
if undeclared:
    failures.append(f'identifiers used but never declared: {undeclared}')

# Every element the renderer reaches for must exist in the markup.
ids = set(re.findall(r"\$\('([^']+)'\)", raw))
present = set(re.findall(r'id="([^"]+)"', html))
missing = sorted(ids - present)
if missing:
    failures.append(f'element ids referenced but not in the markup: {missing}')

# Every endpoint the UI posts to must be routed by the backend.
routes = set(re.findall(r'"(?:GET|POST) (/api/[^"]+)"', open('internal/api/server.go').read()))
calls = sorted(set(re.findall(r"act\('(/api/[^']+)'", raw)))
unrouted = [c for c in calls if c not in routes]
if unrouted:
    failures.append(f'endpoints called but not routed: {unrouted}')

if failures:
    for f in failures:
        print('FAIL:', f, file=sys.stderr)
    sys.exit(1)

print(f'{len(declared)} names, {len(ids)} element ids, {len(calls)} endpoints — all resolve')
PY
