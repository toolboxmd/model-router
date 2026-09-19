"""Deterministic fake CLIs shaped like the real harness contracts.

``FAKE_OPENCODE`` mirrors the OpenCode 1.18.31 server API used by the
runner: Basic auth ``opencode:<password>``, ``directory`` scoping,
``POST /session``, ``POST /session/{id}/prompt_async``, the
``GET /session/status`` map, ``POST /session/{id}/abort``, and
``GET /session/{id}/message``. ``FAKE_CLAUDE`` prints the
``--output-format json`` result object. No model is called.

Environment: ``FAKE_STATE`` (log directory), ``FAKE_OC_MODE`` (free-route
behavior: ok, free_limit, api_free_error, rate_limit, model_text, hang),
``FAKE_OC_DELAY`` (seconds of busy time), ``FAKE_OC_WRITE`` (relative
file the fake worker writes), ``FAKE_CLAUDE_MODE`` (ok, fail, fork),
``FAKE_CLAUDE_ANSWER``.
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
assert "--pure" in argv and "127.0.0.1" in argv and "--port" in argv, argv
PWD = os.environ.get("OPENCODE_SERVER_PASSWORD") or ""
assert PWD, "password must arrive in the environment"
(st / "pwd-in-argv").write_text("yes" if PWD in " ".join(argv) else "no")
MODE = os.environ.get("FAKE_OC_MODE", "ok")
MODE_GO = os.environ.get("FAKE_OC_MODE_GO", "ok")
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
            "variant": "xhigh", "finish": finish}
    if error:
        info["error"] = error
    parts = [{"type": "text", "text": text}] if text else []
    return {"info": info, "parts": parts}

def run_turn(sid, model, directory, aborted):
    free = model.get("providerID") == "opencode"
    mode = MODE if free else (MODE_GO if model.get("providerID") == "opencode-go" else "ok")
    with lock:
        status[sid] = {"type": "busy"}
    if mode == "overloaded":
        # The provider keeps retrying; attempts climb until the runner aborts.
        attempt = 0
        while not aborted.is_set():
            attempt += 1
            with lock:
                status[sid] = {"type": "retry", "attempt": attempt, "message": "Provider overloaded",
                               "action": {"reason": "rate_limit", "provider": model.get("providerID"),
                                          "title": "t", "message": "m", "label": "l"}, "next": 1}
            time.sleep(0.3)
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
            threading.Thread(target=run_turn, args=(sid, body.get("model") or {}, directory, ev), daemon=True).start()
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
mode = os.environ.get("FAKE_CLAUDE_MODE", "ok")
import time
time.sleep(float(os.environ.get("FAKE_CLAUDE_DELAY", "0")))
if mode == "fail":
    sys.stderr.write("planner failed\n")
    sys.exit(1)
if mode == "fork":
    sid = "00000000-0000-4000-8000-000000000000"
print(json.dumps({"type": "result", "subtype": "success", "is_error": False,
                  "result": os.environ.get("FAKE_CLAUDE_ANSWER", "Approved as written."),
                  "session_id": sid}))
'''


def write_fake(bindir, name, body, python):
    path = bindir / name
    path.write_text("#!" + python + "\n" + body, encoding="utf-8")
    path.chmod(0o700)
    return path
