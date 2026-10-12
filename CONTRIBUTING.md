# Contributing to pi-muse

Thanks for helping! Issues and PRs are welcome.

## Ground rules
- **Never commit secrets**: muse2api API keys, muse.ai cookies, `.env`, `muse.json`, account exports, session captures. `.gitignore` covers the usual suspects — double-check `git diff --cached` before pushing.
- Redact session ids, VM ids and auth tokens from logs/captures you attach to issues.
- Keep changes English-only (code comments, docs, messages).

## Dev loop
1. Run muse2api with the `backend/pi_native.py` endpoints (see `backend/README.md`).
2. `pi -e ./src/index.ts --model muse/auto/auto` to load the extension without installing it, or `pi install .` once.
3. After edits, run `/reload` inside Pi.
4. Smoke test: `pi -p --no-session --mode json --model muse/auto/auto "Run: echo hi > t.txt && ls" < /dev/null` and check for `tool_execution_*` events of `muse_tool`.

## Protocol changes
muse.ai is a moving target. If events change shape, update `backend/pi_native.py::normalize()` first and document the new shape in `docs/PROTOCOL.md`.

## Style
TypeScript, tabs, no build step (Pi loads TS via jiti). Python: keep `pi_native.py` dependency-free beyond what muse2api already uses.

## Tests
```bash
ln -s ~/.pi/agent/install/releases/<pi-version>/node_modules node_modules   # Pi's own packages
npm test   # unit tests (classifier, continuation message, lock, recovery) + mock-muse2api integration tests
```
The integration tests use a deterministic fake muse2api (`test/mock-muse2api.ts`); short timeouts and backoffs
are set through env vars inside the tests only.
