# Objective

Deliver authorized software tasks on this Mac from a planner in any harness
to one review-ready final PR, with the dispatcher as the human's point of
contact and the planner engaged only for escalations.

The Objective is complete when:

- A planner in Claude Code, Codex, OpenCode or Grok Build submits through the
  public CLI, and nothing in the runner assumes a planner harness or model.
  The planner's harness, session and model are recorded when known.
- After submission the dispatcher owns the job: workers commit as they work,
  the runner pushes component work, opens stacked component PRs into an
  integration branch, obtains independent review on a route other than the
  implementer's, applies findings through correction turns, merges reviewed
  components, and opens the final PR with the cumulative diff, combined
  proof and review. It never merges a final PR.
- The human, or an agent acting for them, contacts a job's dispatcher
  directly through the public CLI to ask for status, answer its questions,
  give instructions, and receive the final report.
- The planner is engaged only by escalation: when the dispatcher judges a
  problem beyond it, it escalates with evidence to the planner in the
  planner's own harness, or to the human when the planner cannot be reached.
  Progress, routine questions and delivery never reach the planner, and each
  job records every planner engagement and its token use.
- A malformed dispatcher reply costs one repair turn, not the job, and
  installing a new release never breaks a running job.
- Dispatch stays read-only; git and GitHub writes run in write-capable
  runner turns within the project's authority, and secrets never enter the
  ledger.
- Deterministic tests with fake harnesses and a fake GitHub prove submission
  from several planner harnesses, direct dispatcher contact, escalation, and
  the commit, stacked PR, review, correction and final-report paths. One real
  task proves them live from a planner outside Claude Code.
- The release is installed through the marketplace and verified on ordinary
  work.

Merging final PRs, releases, installations and other Human Gates stay with
people. Multi-machine operation, a learned routing policy and dashboards
remain outside this milestone.
