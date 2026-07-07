"""
mock_backends.py
================
Project : Janus — Custom Layer 7 Load Balancer
Role    : Phase 3 — Mock Backend Server Farm

Spins up three independent Flask HTTP servers on ports 8001, 8002, and 8003.
Each server is launched as a separate OS process via multiprocessing so a
single  `python mock_backends.py`  command boots the entire upstream farm.

Usage:
  $ python mock_backends.py          # boots all three, blocks until Ctrl+C
  $ python mock_backends.py 8001     # boot only one specific port (optional)

Per TRD §1: Flask is explicitly permitted for mock backend endpoints only.
The Janus proxy core (janus_core.py) must never import Flask.
"""

import multiprocessing
import signal
import sys
import time

from flask import Flask, Response


# ──────────────────────────────────────────────────────────────────────────────
# BACKEND DEFINITIONS
# Each entry defines a distinct server identity that is served on its port.
# The payload is intentionally verbose so curl / browser output immediately
# confirms which backend answered, making round-robin cycling easy to eyeball.
# ──────────────────────────────────────────────────────────────────────────────

BACKENDS: list[dict] = [
    {
        "id":      "srv_01",
        "port":    8001,
        "label":   "Backend Server 01",
        "banner":  "UPSTREAM-01",
        "color":   "\033[92m",          # bright green (terminal only)
    },
    {
        "id":      "srv_02",
        "port":    8002,
        "label":   "Backend Server 02",
        "banner":  "UPSTREAM-02",
        "color":   "\033[96m",          # bright cyan
    },
    {
        "id":      "srv_03",
        "port":    8003,
        "label":   "Backend Server 03",
        "banner":  "UPSTREAM-03",
        "color":   "\033[93m",          # bright yellow
    },
]

_RESET = "\033[0m"


# ──────────────────────────────────────────────────────────────────────────────
# FLASK APPLICATION FACTORY
# ──────────────────────────────────────────────────────────────────────────────

def _make_app(server_id: str, label: str, port: int) -> Flask:
    """
    Build a minimal Flask application that represents one upstream backend.

    Registered routes:
      GET  /           — health-check / root probe
      GET  /health     — explicit health endpoint (Phase 4 heartbeat target)
      POST /           — accepts any POST body, echoes back server identity
      *    /<path>     — catch-all; returns the same identity payload so any
                         proxied path resolves cleanly during Phase 3 testing.

    The response body is plain text containing the server ID, port, and a
    monotonic request counter so repeated hits are visually distinguishable
    in the terminal (counter climbs each request).
    """
    app = Flask(server_id)
    app.config["SERVER_ID"] = server_id
    counter = {"n": 0}                  # mutable int inside a dict (closure-safe)

    @app.route("/", defaults={"path": ""}, methods=["GET", "POST", "OPTIONS", "PUT"])
    @app.route("/<path:path>",            methods=["GET", "POST", "OPTIONS", "PUT"])
    def catch_all(path: str) -> Response:
        counter["n"] += 1
        req_path = f"/{path}" if path else "/"
        body = (
            f"[ Janus Mock Farm — {label} ]\n"
            f"  Server ID : {server_id}\n"
            f"  Port      : {port}\n"
            f"  Path      : {req_path}\n"
            f"  Request # : {counter['n']}\n"
            f"  Timestamp : {time.strftime('%Y-%m-%dT%H:%M:%S')}\n"
        )
        return Response(
            body,
            status=200,
            mimetype="text/plain",
            headers={
                "X-Served-By": server_id,
                "X-Port":      str(port),
                "Connection":  "close",
            },
        )

    @app.route("/health", methods=["GET"])
    def health() -> Response:
        """Explicit health probe — Phase 4 heartbeat daemon will hit this."""
        return Response(
            f"OK — {server_id} is alive on port {port}\n",
            status=200,
            mimetype="text/plain",
            headers={"X-Served-By": server_id},
        )

    return app


# ──────────────────────────────────────────────────────────────────────────────
# PROCESS WORKER
# ──────────────────────────────────────────────────────────────────────────────

def _run_server(cfg: dict) -> None:
    """
    Worker function executed inside each child process.

    Suppresses Flask's default Werkzeug banner and runs on 127.0.0.1 only
    (loopback-only, matching TRD backend host spec "127.0.0.1").
    use_reloader=False is mandatory — the reloader spawns additional
    subprocesses which conflicts with the multiprocessing model.
    """
    # Silence SIGINT in workers — the parent handles it and terminates children.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    server_id = cfg["id"]
    port      = cfg["port"]
    label     = cfg["label"]
    color     = cfg["color"]

    app = _make_app(server_id, label, port)

    print(
        f"  {color}[{server_id}]{_RESET}  Listening on "
        f"http://127.0.0.1:{port}  — {label}",
        flush=True,
    )

    # werkzeug dev server — suitable for mock/test farm, not for the proxy core.
    app.run(
        host="127.0.0.1",
        port=port,
        debug=False,
        use_reloader=False,
        threaded=True,          # handle multiple concurrent forwarded requests
    )


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Launch all three backend servers as independent OS processes.

    Lifecycle:
      1. Spawn one Process per BACKENDS entry.
      2. Block the parent in a join loop.
      3. On SIGINT (Ctrl+C), terminate all children and exit cleanly.
    """
    # Optional: allow  `python mock_backends.py 8001`  to start a single server.
    target_ports: set[int] = set()
    if len(sys.argv) > 1:
        try:
            target_ports = {int(p) for p in sys.argv[1:]}
        except ValueError:
            print("Usage: python mock_backends.py [port ...]", file=sys.stderr)
            sys.exit(1)

    configs = (
        [c for c in BACKENDS if c["port"] in target_ports]
        if target_ports
        else BACKENDS
    )

    if not configs:
        print("No matching backend configs found.", file=sys.stderr)
        sys.exit(1)

    print("=" * 58, flush=True)
    print("  JANUS MOCK BACKEND FARM — Phase 3 Upstream Targets", flush=True)
    print("=" * 58, flush=True)

    workers: list[multiprocessing.Process] = []
    for cfg in configs:
        p = multiprocessing.Process(
            target=_run_server,
            args=(cfg,),
            name=cfg["id"],
            daemon=True,            # dies automatically when parent exits
        )
        p.start()
        workers.append(p)

    print(f"\n  {len(workers)} backend(s) started.  Press Ctrl+C to stop all.\n",
          flush=True)

    try:
        # Keep the parent alive; workers are daemon processes so they will
        # exit automatically if this process is killed.
        for w in workers:
            w.join()
    except KeyboardInterrupt:
        print("\n  Shutdown signal received — stopping all backends …", flush=True)
        for w in workers:
            w.terminate()
        for w in workers:
            w.join(timeout=3)
        print("  All backends stopped.  Goodbye.", flush=True)


if __name__ == "__main__":
    # Required on Windows: multiprocessing uses 'spawn' by default.
    multiprocessing.freeze_support()
    main()
