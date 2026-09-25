"""Shared test fakes: an in-memory T3 orchestration client. Stdlib only.

Jobs run as T3 child threads (#106, #110). Tests either pass a
``FakeT3Client`` to the controller's ``t3_client`` parameters or install
it for a job with :func:`use_fake_t3`, which also records the job's
dispatcher thread the way a real dispatch would.
"""
import copy
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from runner import store, t3exec

PLANNER = "planner-1"
PROJECT = "proj-1"
NOW = datetime.now(timezone.utc)


def isolate_t3_env():
    """Keep a test module off any live T3 (call from ``setUpModule``).

    Some tests reach T3 discovery without a fake client; inside a runner
    job ``T3_SERVER_URL``/``T3_SERVER_TOKEN`` name the live server, and
    without them the default URL and ``t3 auth`` do. Point both at a
    closed port for the module and restore them afterwards.
    """
    patcher = mock.patch.dict("os.environ", {"T3_SERVER_URL": "http://127.0.0.1:9",
                                             "T3_SERVER_TOKEN": "test-token"})
    patcher.start()
    unittest.addModuleCleanup(patcher.stop)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def msg(text, role="assistant", streaming=False, age_secs=0, turn="t1"):
    ts = iso(NOW - timedelta(seconds=age_secs))
    return {"id": f"m-{role}-{age_secs}", "role": role, "text": text,
            "turnId": turn, "streaming": streaming,
            "createdAt": ts, "updatedAt": ts}


def act(kind, age_secs=0, turn="t1", tone="tool", call=None, task=None):
    payload = {}
    if call is not None:
        payload["toolCallId"] = call
    if task is not None:
        payload["taskId"] = task
    return {"id": f"a-{kind}-{age_secs}-{call or task or ''}", "tone": tone,
            "kind": kind, "summary": kind, "payload": payload, "turnId": turn,
            "createdAt": iso(NOW - timedelta(seconds=age_secs))}


def snap(tid, *, state="running", text="", streaming=False,
         activities=None, session_status="running", last_error=None,
         age_secs=0, project=PROJECT, turn="t1", user_text=None,
         requested_age_secs=None):
    session = {"threadId": tid, "status": session_status,
               "providerName": "test", "runtimeMode": "full-access",
               "activeTurnId": turn if state == "running" else None,
               "lastError": last_error,
               "updatedAt": iso(NOW - timedelta(seconds=age_secs))}
    messages = []
    if user_text is not None:
        messages.append(msg(user_text, role="user", age_secs=age_secs,
                            turn=turn))
    if text or streaming:
        messages.append(msg(text, streaming=streaming, age_secs=age_secs,
                            turn=turn))
    requested = iso(NOW - timedelta(
        seconds=(age_secs if requested_age_secs is None
                 else requested_age_secs)))
    return {"snapshotSequence": 1,
            "thread": {"id": tid, "projectId": project,
                       "messages": messages,
                       "activities": list(activities or []),
                       "session": session,
                       "latestTurn": {"turnId": turn, "state": state,
                                     "requestedAt": requested,
                                     "startedAt": requested,
                                     "completedAt": (iso(NOW) if state == "completed"
                                                     else None),
                                     "assistantMessageId": None}}}


def planner_snap():
    return snap(PLANNER, state="completed", text="planning here",
                project=PROJECT)


class FakeT3Client:
    """In-memory orchestration server behind the T3Client interface."""

    def __init__(self, planner=PLANNER, project=PROJECT):
        self.planner = planner
        self.project = project
        self.commands = []
        self.posts = []
        self.reads = {}
        self.scripts = {}
        self.dispatch_error = None
        # Planner replies: each post into the planner thread consumes one
        # (None = the planner never answers); the reply lands as a new
        # completed turn after the posted user message.
        self.planner_replies = []
        self.planner_messages = []
        self.planner_turns = 0
        # Child replies: each post onto a child thread consumes one and
        # completes a new turn with that text.
        self.child_replies = []
        self.child_turns = {}
        # Reply used once ``child_replies`` runs out (None: the child turn
        # never completes, which a bounded watch reports as stalled).
        self.default_child_reply = None
        # Prism provider snapshot served to the router (None: an older
        # server without the endpoint, answered with HTTP 404).
        self.prism = None
        self.prism_reads = []

    def dispatch(self, command):
        self.commands.append(copy.deepcopy(command))
        if self.dispatch_error is not None:
            raise self.dispatch_error
        ctype = command.get("type")
        if ctype == "thread.create":
            self.scripts.setdefault(command["threadId"], [])
        elif ctype == "thread.turn.interrupt":
            self.scripts[command["threadId"]] = snap(
                command["threadId"], state="interrupted")
        return {"sequence": len(self.commands)}

    def create_child(self, child_id, parent_thread_id, project_id,
                     title, route, role="implementation"):
        return self.dispatch(t3exec.child_create_command(
            child_id, parent_thread_id, project_id, title, route, role))

    def post_message(self, thread_id, text, route=None, role="implementation",
                     title_seed=None, message_id=None):
        self.posts.append((thread_id, text))
        cmd = t3exec.turn_start_command(thread_id, text, route, role,
                                        title_seed, message_id)
        out = self.dispatch(cmd)
        if thread_id == self.planner:
            self.planner_messages.append(
                {**msg(text, role="user", turn=None),
                 "id": cmd["message"]["messageId"]})
            reply = self.planner_replies.pop(0) if self.planner_replies else None
            if reply is not None:
                self.planner_turns += 1
                self.planner_messages.append(
                    msg(reply, turn=f"pt{self.planner_turns}"))
        elif self.child_replies or self.default_child_reply is not None:
            n = self.child_turns.get(thread_id, 0) + 1
            self.child_turns[thread_id] = n
            text = (self.child_replies.pop(0) if self.child_replies
                    else self.default_child_reply)
            self.scripts[thread_id] = snap(thread_id, state="completed",
                                           text=text, turn=f"ct{n}")
        return out

    def thread_snapshot(self, thread_id):
        self.reads[thread_id] = self.reads.get(thread_id, 0) + 1
        if thread_id == self.planner:
            base = planner_snap()
            base["thread"]["messages"] += copy.deepcopy(self.planner_messages)
            if self.planner_turns:
                base["thread"]["latestTurn"]["turnId"] = f"pt{self.planner_turns}"
            return base
        script = self.scripts.get(thread_id)
        if isinstance(script, list) and script:
            return script.pop(0)
        if isinstance(script, dict):
            return copy.deepcopy(script)
        return snap(thread_id, state="running", age_secs=0)

    def prism_snapshot(self, project_id=None):
        self.prism_reads.append(project_id)
        if self.prism is None:
            raise t3exec.T3Error("T3 GET /api/prism/snapshot failed: HTTP 404")
        return copy.deepcopy(self.prism)

    def complete(self, thread_id, text):
        self.scripts[thread_id] = snap(thread_id, state="completed",
                                       text=text)


def prism_provider(instance, models, driver=None, enabled=True, windows=None):
    """One provider entry of a Prism snapshot (toolboxmd/t3code#19 shape)."""
    entry = {"instanceId": instance, "driver": driver or instance, "enabled": enabled,
             "status": "ready",
             "models": [{"slug": m, "name": m, "isCustom": False, "capabilities": None}
                        for m in models]}
    if windows is not None:
        entry["usageLimits"] = {"checkedAt": iso(NOW), "windows": windows}
    return entry


def prism_snapshot(providers, lanes=None):
    """A Prism snapshot: providers plus role kits with ``lanes`` per role."""
    roles = {role: {"instructions": "", "skills": [], "threadTools": "none",
                    "lanes": {"easy": [], "medium": [], "hard": []}}
             for role in ("planner", "dispatcher", "reviewer", "worker",
                          "correction", "recovery")}
    for role, per_lane in (lanes or {}).items():
        roles[role]["lanes"].update(per_lane)
    return {"generatedAt": iso(NOW), "projectId": PROJECT, "providers": providers,
            "roles": roles}


def use_fake_t3(testcase, sd, rid, replies=None, planner=None, default=None):
    """Route one job's T3 calls to a fake and save its dispatcher thread.

    ``replies`` are the dispatcher's next assistant texts and ``default``
    answers every later turn (dicts are sent as JSON envelopes). Returns
    the fake for further scripting.
    """
    con = store.connect(sd)
    try:
        row = con.execute("SELECT planner_t3_thread, controller_state FROM jobs"
                          " WHERE request_id=?", (rid,)).fetchone()
        planner = planner or row["planner_t3_thread"]
        st = json.loads(row["controller_state"] or "{}")
        threads = st.setdefault("t3_threads", {})
        threads.setdefault("dispatch", {"thread_id": f"sub.{planner}.disp",
                                        "route": "luna/max"})
        con.execute("UPDATE jobs SET controller_state=? WHERE request_id=?",
                    (json.dumps(st, sort_keys=True), rid))
    finally:
        con.close()
    fake = FakeT3Client(planner=planner)
    fake.child_replies = [r if isinstance(r, str) else json.dumps(r)
                          for r in (replies or [])]
    if default is not None:
        fake.default_child_reply = (default if isinstance(default, str)
                                    else json.dumps(default))
    patcher = mock.patch.object(t3exec, "client_for_job", lambda *a, **k: fake)
    patcher.start()
    testcase.addCleanup(patcher.stop)
    return fake
