---
name: model-router
description: Hand an authorized software task to Model Router, which runs it as T3 threads and returns one open PR. Use when a planner hosted in a T3 thread delegates implementation work.
---

# Model Router

Project instructions own delegation, authority, proof, review, and delivery.

Run the bundled `bin/model-router` two directories above this Skill, from the
target workspace. Submit from a planner hosted in a T3 thread:

```sh
model-router submit --request-id <id> --task-file <task.json> \
  --workspace <repo> --planner-session <session> \
  --planner-t3-thread <thread-id> --start
```

Follow the job with `status`, `questions`, `answer`, `result`, `cancel` and
`recover`; `capacity` shows route marks and the T3 snapshot view. Chromeria's
Prism picks models and limits; do not choose them here. `RUNNER.md` in the
plugin root is the full reference.
