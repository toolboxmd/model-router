# Objective

Deliver authorized software tasks on this Mac from a planner in any harness
to one open PR, with the dispatcher delivering and the planner woken only
when the dispatcher needs its judgment or a job ends.

The Objective is complete when:

- A planner in any harness submits through the public CLI, and nothing in
  the runner assumes a planner harness, model or session.
- Workers commit as they work, and each job ends with one pushed branch and
  one open PR carrying the proof and the dispatcher's review; only a person
  merges it.
- When the dispatcher needs judgment it cannot supply, the runner wakes the
  planner automatically in the planner's own harness; the human is never on
  the critical path and can still answer or steer through the CLI.
- Every job's end state reaches the planner in its own thread, even while
  that thread is open: ready to merge with the PR URL, or blocked, failed
  or cancelled with the reason.
- A dispatcher reply missing its closing braces no longer blocks a job.
- One real task handed over from a planner in a T3 thread (any harness)
  runs its dispatcher and workers as T3 threads of that planner thread and
  reaches an open PR.

Merging, releases, installations and other Human Gates stay with people.
Stacked PR trains, multi-machine operation and a learned routing policy
remain outside this milestone.
