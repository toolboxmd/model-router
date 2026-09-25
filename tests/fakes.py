"""Deterministic fake CLIs shaped like the real harness contracts.

``FAKE_OPENCODE`` mirrors the OpenCode 1.18.31 server API used by the
runner: Basic auth ``opencode:<password>``, ``directory`` scoping,
``POST /session``, ``POST /session/{id}/prompt_async``, the
``GET /session/status`` map, ``POST /session/{id}/abort``, and
``GET /session/{id}/message``. ``FAKE_CLAUDE`` prints the
``--output-format json`` result object. ``FAKE_GROK`` mirrors the Grok
Build headless contract: ``grok -p PROMPT --cwd WS -m MODEL
--output-format json [--resume SESSION]`` printing one JSON result with
``text``, ``stopReason``, ``sessionId`` and ``num_turns``, or
``{"type": "error", "message": ...}`` with a non-zero exit. No model is
called.

Environment: ``FAKE_STATE`` (log directory), ``FAKE_OC_MODE`` (free-route
behavior: ok, free_limit, api_free_error, rate_limit, model_text, hang,
stall, idle_incomplete, idle_empty, hard_error, context_error), ``FAKE_OC_MODE_GO`` (same for Go routes),
``FAKE_OC_PROBE_MODE`` (stall-probe answer: ok, exhausted, overloaded),
``FAKE_OC_DELAY`` (seconds of busy time), ``FAKE_OC_WRITE`` (relative file the fake worker writes), ``FAKE_CLAUDE_MODE`` (ok, fail, fork),
``FAKE_CLAUDE_ANSWER``, ``FAKE_GROK_MODE`` (ok, exhaustion, overload,
hard_error, hang, hold), ``FAKE_GROK_DELAY`` (seconds before success),
``FAKE_GROK_WRITE`` (relative file the fake worker writes),
``FAKE_GROK_RELEASE`` (hold mode waits for this file, then succeeds).
``FAKE_OC_TOOLS`` (``1`` appends one ``read`` tool part to text assistant
messages, proving tool-part recording; unset keeps text-only turns).
``FAKE_OC_PLAN`` (plan-agent envelope sequence: ``completion`` (default)
answers every prompt with a completion envelope; ``implement_then_complete``
answers the session's first prompt with an implementation envelope and later
prompts with completion, so a drill runs dispatch, worker, resume, done;
``implement_twice_then_complete`` answers the first two prompts with
implementation envelopes and later prompts with completion, so a drill runs
dispatch, worker, correction, resume, done).
``FAKE_OC_FENCE`` (``1`` wraps the plan envelope in a ```json fence).
"""

FAKE_OPENCODE = r'''
import base64, json, os, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
st = Path(os.environ["FAKE_STATE"])
st.mkdir(parents=True, exist_ok=True)
argv = sys.argv[1:]
with open(st / "opencode.log", "a") as f:
    f.write(json.dumps(argv) + "\n")
if not argv or argv[0] != "serve":
    print(json.dumps({"type": "error", "error": {"name": "UnknownError", "data": {"message": "run not faked"}}}))
    sys.exit(1)
assert "--pure" not in argv and "127.0.0.1" in argv and "--port" in argv, argv
PWD = os.environ.get("OPENCODE_SERVER_PASSWORD") or ""
assert PWD, "password must arrive in the environment"
(st / "pwd-in-argv").write_text("yes" if PWD in " ".join(argv) else "no")
kit_env = {k: os.environ.get(k, "") for k in ("OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME", "OPENCODE_CONFIG", "OPENCODE_DISABLE_EXTERNAL_SKILLS")}
with open(st / "opencode-env.jsonl", "a") as f:
    f.write(json.dumps(kit_env) + "\n")
try:
    _kd = kit_env.get("OPENCODE_CONFIG_DIR") or ""
    if _kd:
        _kj = Path(_kd) / "kit.json"
        if _kj.is_file():
            (st / "kit-actual.json").write_text(_kj.read_text())
        _oj = Path(_kd) / "opencode.json"
        if _oj.is_file():
            (st / "opencode-kit-config.json").write_text(_oj.read_text())
except Exception:
    pass
MODE = os.environ.get("FAKE_OC_MODE", "ok")
AGENT = None  # set per prompt from the request body
MODE_GO = os.environ.get("FAKE_OC_MODE_GO", "ok")
try:
    NEXT_OVERRIDE = float(os.environ.get("FAKE_OC_NEXT", ""))
except ValueError:
    NEXT_OVERRIDE = None
except Exception:
    NEXT_OVERRIDE = None
DELAY = float(os.environ.get("FAKE_OC_DELAY", "0.3"))
WRITE = os.environ.get("FAKE_OC_WRITE")
EXPECT = "Basic " + base64.b64encode(("opencode:" + PWD).encode()).decode()
lock = threading.Lock()
sessions = {}
status = {}
counter = [0]

def nid(prefix):
    counter[0] += 1
    return "%s_%06d" % (prefix, counter[0])

def log(obj):
    with open(st / "opencode-requests.jsonl", "a") as f:
        f.write(json.dumps(obj) + "\n")

def assistant(sid, model, text="", error=None, finish="stop"):
    info = {"id": nid("msg"), "sessionID": sid, "role": "assistant",
            "time": {"created": 1, "completed": 2},
            "providerID": model.get("providerID"), "modelID": model.get("modelID"),
            "variant": "xhigh", "finish": finish,
            "tokens": {"input": 100, "output": 20, "reasoning": 5, "cache": {"read": 300, "write": 0}},
            "cost": 0.0}
    if error:
        info["error"] = error
    parts = [{"type": "text", "text": text}] if text else []
    if text and os.environ.get("FAKE_OC_TOOLS") == "1":
        # Opt-in tool part so tests can prove distinct tool-part recording;
        # default off keeps every existing text-only drill unchanged.
        parts.append({"type": "tool", "name": "read",
                      "input": {"path": "VISION.md"}})
    return {"info": info, "parts": parts}

def run_turn(sid, model, directory, aborted, prompt_text=""):
    free = model.get("providerID") == "opencode"
    mode = MODE if free else (MODE_GO if model.get("providerID") == "opencode-go" else "ok")
    if "stall probe" in (prompt_text or "").lower():
        # A stall probe answers fast on its own, never repeating the turn's
        # mode: FAKE_OC_PROBE_MODE ok (default), exhausted, or overloaded.
        pmode = os.environ.get("FAKE_OC_PROBE_MODE", "ok")
        time.sleep(0.2)
        if pmode == "exhausted":
            msg = assistant(sid, model, error={"name": "APIError", "data": {
                "message": "go exceeded", "statusCode": 429, "isRetryable": False,
                "responseBody": json.dumps({"type": "error", "error": {"type": "GoUsageLimitError"}})}})
        elif pmode == "overloaded":
            msg = assistant(sid, model, error={"name": "APIError", "data": {
                "message": "rate limited", "statusCode": 429, "isRetryable": True,
                "responseBody": json.dumps({"type": "error", "error": {"type": "RateLimitError"}})}})
        else:
            msg = assistant(sid, model, text="ok")
        with lock:
            sessions[sid].append(msg)
            status.pop(sid, None)
        return
    if mode == "idle_incomplete":
        # #88 review fixture: stream incomplete assistant messages while
        # busy (each part refreshes the drive loop's activity tracker, so
        # no busy-branch stall fires), then go idle leaving the last new
        # assistant message without time.completed or info.error. The
        # drive loop must end the turn through the stream-silence stall
        # machinery, never poll forever.
        with lock:
            status[sid] = {"type": "busy"}
        for i in range(5):
            time.sleep(0.5)
            if aborted.is_set():
                break
            with lock:
                incomplete = {"id": nid("msg"), "sessionID": sid, "role": "assistant",
                              "time": {"created": 1},
                              "providerID": model.get("providerID"),
                              "modelID": model.get("modelID"),
                              "variant": "xhigh", "finish": None,
                              "tokens": {"input": 100, "output": 5, "reasoning": 2,
                                         "cache": {"read": 0, "write": 0}},
                              "cost": 0.0}
                sessions[sid].append({"info": incomplete,
                                      "parts": [{"type": "text",
                                                 "text": "partial work %d, never completed" % i}]})
        if not aborted.is_set():
            with lock:
                status.pop(sid, None)
        while not aborted.is_set():
            time.sleep(0.05)
        with lock:
            sessions[sid].append(assistant(sid, model, error={"name": "MessageAbortedError", "data": {"message": "aborted"}}))
            status.pop(sid, None)
        return
    if mode == "idle_empty":
        # #88 review variant: report busy long enough for the drive loop
        # to observe it (one poll per second) but shorter than the test
        # silence window, then go idle with no new assistant message at
        # all. Same stall expectation as idle_incomplete.
        with lock:
            status[sid] = {"type": "busy"}
        time.sleep(1.5)
        if not aborted.is_set():
            with lock:
                status.pop(sid, None)
        while not aborted.is_set():
            time.sleep(0.05)
        with lock:
            sessions[sid].append(assistant(sid, model, error={"name": "MessageAbortedError", "data": {"message": "aborted"}}))
            status.pop(sid, None)
        return
    if mode == "stall":
        # Stream a few parts, then go silent while reporting busy: the
        # supervisor must end the turn on the silence window, not the timeout.
        with lock:
            status[sid] = {"type": "busy"}
        for i in range(3):
            time.sleep(0.5)
            if aborted.is_set():
                break
            with lock:
                sessions[sid].append(assistant(sid, model, text="stream part %d" % i))
        while not aborted.is_set():
            time.sleep(0.05)
        with lock:
            sessions[sid].append(assistant(sid, model, error={"name": "MessageAbortedError", "data": {"message": "aborted"}}))
            status.pop(sid, None)
        return
    with lock:
        status[sid] = {"type": "busy"}
    if mode in ("transport_503", "transport_529", "http_503", "http_529"):
        code = 503 if "503" in mode else 529
        with lock:
            status[sid] = {"type": "busy", "transport_code": code}
        while not aborted.is_set():
            time.sleep(0.05)
        with lock:
            sessions[sid].append(assistant(sid, model, error={"name": "MessageAbortedError", "data": {"message": "aborted"}}))
            status.pop(sid, None)
        return
    if mode == "overloaded":
        # The provider keeps retrying; attempts climb until the runner aborts.
        nxt = NEXT_OVERRIDE if NEXT_OVERRIDE is not None else 1
        attempt = 0
        while not aborted.is_set():
            attempt += 1
            with lock:
                status[sid] = {"type": "retry", "attempt": attempt, "message": "Provider overloaded",
                               "action": {"reason": "rate_limit", "provider": model.get("providerID"),
                                          "title": "t", "message": "m", "label": "l"}, "next": nxt}
            time.sleep(0.3)
        with lock:
            sessions[sid].append(assistant(sid, model, error={"name": "MessageAbortedError", "data": {"message": "aborted"}}))
            status.pop(sid, None)
        return
    if mode == "sticky_retry":
        # One retry episode with a constant attempt number across several
        # polls, then success. The runner must count a single attempt.
        # 3.5s covers at least three 1s supervisor polls with the same attempt.
        with lock:
            status[sid] = {"type": "retry", "attempt": 1, "message": "Provider overloaded",
                           "action": {"reason": "rate_limit", "provider": model.get("providerID"),
                                      "title": "t", "message": "m", "label": "l"}, "next": 1}
        for _ in range(35):
            if aborted.is_set():
                break
            time.sleep(0.1)
        if aborted.is_set():
            with lock:
                sessions[sid].append(assistant(sid, model, error={"name": "MessageAbortedError", "data": {"message": "aborted"}}))
                status.pop(sid, None)
            return
        if WRITE:
            Path(directory, WRITE).write_text("implemented by fake worker\n")
        msg = assistant(sid, model, text="IMPLEMENTED by fake worker")
        with lock:
            sessions[sid].append(msg)
            status.pop(sid, None)
        return
    if mode == "three_retries":
        # Three distinct retry episodes (attempt 1, 2, 3) separated by busy
        # gaps longer than the supervisor's 1s poll, so each episode is
        # observed. The runner allows two and aborts on the third.
        for att in (1, 2, 3):
            with lock:
                status[sid] = {"type": "retry", "attempt": att, "message": "Provider overloaded",
                               "action": {"reason": "rate_limit", "provider": model.get("providerID"),
                                          "title": "t", "message": "m", "label": "l"}, "next": 1}
            for _ in range(12):
                if aborted.is_set():
                    break
                time.sleep(0.1)
            if aborted.is_set():
                break
            if att < 3:
                with lock:
                    status[sid] = {"type": "busy"}
                for _ in range(12):
                    if aborted.is_set():
                        break
                    time.sleep(0.1)
                if aborted.is_set():
                    break
        while not aborted.is_set():
            time.sleep(0.05)
        with lock:
            sessions[sid].append(assistant(sid, model, error={"name": "MessageAbortedError", "data": {"message": "aborted"}}))
            status.pop(sid, None)
        return
    if mode in ("free_limit", "hang"):
        if mode == "free_limit":
            with lock:
                status[sid] = {"type": "retry", "attempt": 1, "message": "Free usage exceeded, subscribe to Go",
                               "action": {"reason": "free_tier_limit", "provider": "opencode",
                                          "title": "Free limit", "message": "Free usage exceeded",
                                          "label": "Subscribe"}, "next": 999999}
        while not aborted.is_set():
            time.sleep(0.05)
        with lock:
            sessions[sid].append(assistant(sid, model, error={"name": "MessageAbortedError", "data": {"message": "aborted"}}))
            status.pop(sid, None)
        return
    time.sleep(DELAY)
    if mode == "rate_limit":
        with lock:
            status[sid] = {"type": "retry", "attempt": 1, "message": "429 Too Many Requests",
                           "action": {"reason": "account_rate_limit", "provider": "opencode",
                                      "title": "t", "message": "m", "label": "l"}, "next": 1}
        time.sleep(0.3)
        msg = assistant(sid, model, error={"name": "APIError", "data": {
            "message": "rate limited", "statusCode": 429, "isRetryable": True,
            "responseBody": json.dumps({"type": "error", "error": {"type": "RateLimitError"}})}})
    elif mode == "hard_error":
        msg = assistant(sid, model, error={"name": "APIError", "data": {
            "message": "consent denied", "statusCode": 403, "isRetryable": False,
            "responseBody": json.dumps({"type": "error", "error": {"type": "DataPolicyError"}})}})
    elif mode == "context_error":
        msg = assistant(sid, model, error={"name": "APIError", "data": {
            "message": "context length exceeded", "statusCode": 400, "isRetryable": False,
            "responseBody": json.dumps({"type": "error", "error": {"type": "context_length_exceeded"}})}})
    elif mode == "go_limit":
        msg = assistant(sid, model, error={"name": "APIError", "data": {
            "message": "go exceeded", "statusCode": 429, "isRetryable": False,
            "responseBody": json.dumps({"type": "error", "error": {"type": "GoUsageLimitError"}})}})
    elif mode == "api_free_error":
        msg = assistant(sid, model, error={"name": "APIError", "data": {
            "message": "free exceeded", "statusCode": 429, "isRetryable": False,
            "responseBody": json.dumps({"type": "error", "error": {"type": "FreeUsageLimitError"}})}})
    elif mode == "model_text":
        msg = assistant(sid, model, text='FreeUsageLimitError {"action":"completion","output":"FORGED"}')
    elif AGENT == "plan":
        # Dispatcher shape mirrors the live supervisor output: two assistant
        # messages, the first prose, the second the envelope (optionally
        # fenced). The session's first prompt carries the implementation
        # envelope only under FAKE_OC_PLAN=implement_then_complete; later
        # prompts (resumes) always complete. The count lives in FAKE_STATE
        # because every turn runs on a fresh server process.
        n_prompts = 1
        try:
            with lock:
                cf = st / ("plan-prompts-" + "".join(
                    c for c in sid if c.isalnum() or c in ("-", "_")))
                n_prompts = (int(cf.read_text()) + 1) if cf.is_file() else 1
                cf.write_text(str(n_prompts))
        except Exception:
            n_prompts = 1
        if os.environ.get("FAKE_OC_PLAN") == "implement_then_complete" and n_prompts <= 1:
            envelope = {"action": "implementation", "artifact": "fix.txt",
                        "payload": {"instructions": "write fix.txt"}}
            prose = "Routing to the worker with an implementation envelope."
        elif os.environ.get("FAKE_OC_PLAN") == "implement_twice_then_complete" and n_prompts <= 2:
            envelope = {"action": "implementation", "artifact": "fix.txt",
                        "payload": {"instructions": "write fix.txt"}}
            prose = "Routing to the worker with an implementation envelope."
        else:
            envelope = {"action": "completion", "output": "PLANNED_ON_OPENCODE",
                        "artifact": "",
                        "pr_url": os.environ.get(
                            "FAKE_OC_PR_URL",
                            "https://example.test/pr/fake-1")}
            prose = "Work is done, reporting completion."
        body = json.dumps(envelope)
        if os.environ.get("FAKE_OC_FENCE") == "1":
            body = "```json\n" + body + "\n```"
        with lock:
            sessions[sid].append(assistant(sid, model, text=prose))
            sessions[sid].append(assistant(sid, model, text=body))
            status.pop(sid, None)
        return
    else:
        if WRITE:
            Path(directory, WRITE).write_text("implemented by fake worker\n")
        msg = assistant(sid, model, text="IMPLEMENTED by fake worker")
    with lock:
        sessions[sid].append(msg)
        status.pop(sid, None)

aborts = {}

class H(BaseHTTPRequestHandler):
    def _json(self, code, obj=None):
        data = b"" if obj is None else json.dumps(obj).encode()
        self.send_response(code)
        if obj is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if data:
            self.wfile.write(data)
    def _body(self):
        n = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(n) if n else b""
        return json.loads(raw.decode() or "null") if raw else None
    def _pre(self, method):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        body = self._body() if method == "POST" else None
        log({"method": method, "path": u.path, "query": {k: v[0] for k, v in q.items()}, "body": body,
             "auth_ok": self.headers.get("Authorization") == EXPECT})
        if self.headers.get("Authorization") != EXPECT:
            self._json(401, {"error": "unauthorized"})
            return None
        return u.path, q, body
    def do_GET(self):
        got = self._pre("GET")
        if got is None:
            return
        path, q, _ = got
        if path == "/global/health":
            return self._json(200, {"healthy": True, "version": "fake"})
        if path == "/session/status":
            with lock:
                snap = dict(status)
            for _sid, _st in snap.items():
                if isinstance(_st, dict) and _st.get("transport_code") in (503, 529):
                    code = int(_st["transport_code"])
                    return self._json(code, {"type": "error",
                                             "error": {"type": "overloaded_error",
                                                       "message": "transport overloaded"}})
            with lock:
                return self._json(200, dict(status))
        if path.startswith("/session/") and path.endswith("/message"):
            sid = path.split("/")[2]
            with lock:
                return self._json(200, list(sessions.get(sid, [])))
        self._json(404, {"error": "missing"})
    def do_POST(self):
        got = self._pre("POST")
        if got is None:
            return
        path, q, body = got
        directory = (q.get("directory") or [""])[0]
        if path == "/session":
            sid = nid("ses")
            with lock:
                sessions[sid] = []
            return self._json(200, {"id": sid, "directory": directory})
        if path.endswith("/prompt_async"):
            sid = path.split("/")[2]
            with lock:
                sessions.setdefault(sid, []).append({"info": {"id": nid("msg"), "role": "user", "sessionID": sid}, "parts": body.get("parts")})
            ev = threading.Event()
            aborts[sid] = ev
            globals()["AGENT"] = body.get("agent")
            parts = body.get("parts") or []
            text = " ".join(p.get("text", "") for p in parts if isinstance(p, dict))
            threading.Thread(target=run_turn, args=(sid, body.get("model") or {}, directory, ev, text), daemon=True).start()
            return self._json(204)
        if path.endswith("/abort"):
            sid = path.split("/")[2]
            (st / "aborted").write_text(sid)
            if sid in aborts:
                aborts[sid].set()
            return self._json(200, True)
        self._json(404, {"error": "missing"})
    def log_message(self, *a):
        return

srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
print("opencode server listening on http://127.0.0.1:%s" % srv.server_address[1], flush=True)
srv.serve_forever(poll_interval=0.05)
'''

FAKE_CLAUDE = r'''
import json, os, sys
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
st.mkdir(parents=True, exist_ok=True)
argv = sys.argv[1:]
with open(st / "claude.log", "a") as f:
    f.write(json.dumps({"argv": argv, "cwd": os.getcwd()}) + "\n")
assert "--resume" in argv and "--fork-session" not in argv, argv
assert argv[argv.index("--output-format") + 1] == "json", argv
sid = argv[argv.index("--resume") + 1]
import time
time.sleep(float(os.environ.get("FAKE_CLAUDE_DELAY", "0")))
mode = os.environ.get("FAKE_CLAUDE_MODE", "ok")
if mode == "fail":
    sys.stderr.write("planner failed\n")
    sys.exit(1)
if mode == "fork":
    sid = "00000000-0000-4000-8000-000000000000"
print(json.dumps({"type": "result", "subtype": "success", "is_error": False,
                  "result": os.environ.get("FAKE_CLAUDE_ANSWER", "Approved as written."),
                  "session_id": sid, "uuid": "fake-result-uuid", "duration_ms": 12,
                  "num_turns": 1, "total_cost_usd": 0.0,
                  "usage": {"input_tokens": 10, "output_tokens": 3,
                            "cache_read_input_tokens": 120,
                            "cache_creation_input_tokens": 7}}))
'''


FAKE_GROK = r'''
import json, os, sys, time
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
st.mkdir(parents=True, exist_ok=True)
argv = sys.argv[1:]
with open(st / "grok.log", "a") as f:
    f.write(json.dumps({"argv": argv, "cwd": os.getcwd()}) + "\n")

def flag(name):
    return argv[argv.index(name) + 1] if name in argv and argv.index(name) + 1 < len(argv) else None

assert "--cwd" in argv, argv
assert "--output-format" in argv and flag("--output-format") == "json", argv
assert "-p" in argv or "--prompt-file" in argv, argv
cwd = Path(flag("--cwd"))
model = flag("-m") or flag("--model") or "grok-4.6"
resume = flag("--resume")
MODE = os.environ.get("FAKE_GROK_MODE", "ok")
WRITE = os.environ.get("FAKE_GROK_WRITE")
sessions = st / "grok-sessions"
sessions.mkdir(exist_ok=True)
if resume:
    sid = resume
else:
    sid = "ses_grok_%06d" % (len(list(sessions.iterdir())) + 1)
    (sessions / sid).write_text("created")

def success():
    if WRITE:
        (cwd / WRITE).write_text("implemented by fake grok worker\n")
    print(json.dumps({"text": "IMPLEMENTED by fake grok worker", "stopReason": "end_turn",
                      "sessionId": sid, "num_turns": 1, "model": model,
                      "usage": {"input_tokens": 60, "output_tokens": 12}}))

if MODE == "hang":
    time.sleep(120)
    success()
elif MODE == "hold":
    release = os.environ.get("FAKE_GROK_RELEASE", "")
    end = time.monotonic() + 120
    while time.monotonic() < end:
        if release and Path(release).exists():
            break
        time.sleep(0.05)
    success()
elif MODE == "exhaustion":
    print(json.dumps({"type": "error",
                      "message": "xAI subscription quota exceeded: insufficient_quota"}))
    sys.exit(1)
elif MODE == "overload":
    print(json.dumps({"type": "error",
                      "message": "xAI rate limit exceeded: RateLimitError, retry later"}))
    sys.exit(1)
elif MODE == "hard_error":
    print(json.dumps({"type": "error",
                      "message": "context_length_exceeded: maximum context length exceeded"}))
    sys.exit(1)
else:
    time.sleep(float(os.environ.get("FAKE_GROK_DELAY", "0.2")))
    success()
'''


VERSION_GUARD = (
    "import sys as _vs\n"
    "if _vs.argv[1:2] == ['--version']:\n"
    "    print('fake-harness 0.0.0'); raise SystemExit(0)\n"
)


def write_fake(bindir, name, body, python):
    path = bindir / name
    path.write_text("#!" + python + "\n" + VERSION_GUARD + body, encoding="utf-8")
    path.chmod(0o700)
    return path
