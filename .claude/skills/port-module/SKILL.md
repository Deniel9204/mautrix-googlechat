---
name: port-module
description: Port one maugclib/Python-bridge module to Go with fidelity checks. Use for every M1-M7 task that reimplements existing Python behavior.
---

# Port a Python module to Go

1. **Read the spec first**: the relevant section of `docs/research/01` (client lib),
   `02` (proto/pblite), or `03` (bridge features). Then read the ENTIRE Python source
   module named in the task (in `../_reference/googlechat-python/`).
2. **Read the megabridge counterpart** (`../_reference/googlechat-megabridge/`) if one
   exists. Check `docs/research/08c`/`08d` for its known defects in this area — never
   copy a listed defect.
3. **Write a behavior inventory** (comment block or scratch note): every branch,
   error path, header, magic constant, and quirk in the Python original.
4. **TDD**: write table-driven tests from the inventory first, then port. Match the
   task's stated Go signatures exactly — other tasks depend on them.
5. **Fidelity checklist before declaring done**:
   - [ ] every Python error path has a Go equivalent (compare `except` blocks)
   - [ ] magic constants copied exactly (API key, client_version, URLs, headers)
   - [ ] UTF-16 code-unit offsets wherever Python used them
   - [ ] proto2 presence checks (`HasField` -> `!= nil` / `Has` via protoreflect)
   - [ ] no behavior added that the task didn't ask for
6. **Audit**: dispatch the `gchat-port-auditor` agent with (Go files, Python source
   paths). Fix findings, re-audit until clean.
