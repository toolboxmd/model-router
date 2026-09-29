# model-router Repository Instructions

Follow the installed AgentsMD global contract.

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
