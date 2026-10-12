# model-router Repository Instructions

Follow the installed AgentsMD global contract.

This repository is retired: Chromeria's Prism absorbed Model Router. Make no
further releases or installs.

- Final PR merges stay with the user. After the user merges, Delivery
  Authority for this repository includes, without another prompt: the
  resulting GitHub release through the release workflow, and installing that
  released version on the maintainer's local hosts once Toolybara promotes it
  to the marketplace (today the Codex plugin that Chromeria's Prism runs:
  `codex plugin marketplace upgrade`, then
  `codex plugin add model-router@toolboxmd`), with post-install verification
  (`bin/model-router --version` in the installed plugin root) and an entry in
  the maintainer's runbook. Third-party publication still needs explicit
  authority.

## Connected projects

Chromeria's `GLOSSARY.md` is the system glossary for Chromeria, AgentsMD,
Agent Observer and ContextMD.

| Project | Repository | Glossary |
| --- | --- | --- |
| Chromeria (system glossary) | `toolboxmd/chromeria` | [`GLOSSARY.md`](https://github.com/toolboxmd/chromeria/blob/main/GLOSSARY.md) |
| AgentsMD | `toolboxmd/agentsmd` | [`GLOSSARY.md`](https://github.com/toolboxmd/agentsmd/blob/main/GLOSSARY.md) |
| Agent Observer | `toolboxmd/agent-observer` | [`GLOSSARY.md`](https://github.com/toolboxmd/agent-observer/blob/main/GLOSSARY.md) |
| ContextMD | `toolboxmd/contextmd` | [`GLOSSARY.md`](https://github.com/toolboxmd/contextmd/blob/main/GLOSSARY.md) |
