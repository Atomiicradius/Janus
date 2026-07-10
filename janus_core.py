"""
janus_core.py
=============
Project : Janus — Custom Layer 7 Load Balancer
Phase   : 6 — Telemetry JSON APIs & Y2K CRT Admin Dashboard

Constraints (strictly honoured):
  • No FastAPI / asyncio / http.server / requests / urllib
  • Only Python stdlib: socket, selectors, threading, time, signal, sys
  • All state lives in volatile RAM — zero disk persistence
  • Single-threaded I/O multiplexing via selectors.DefaultSelector

Boot sequence
─────────────
  1. Allocate and bind a master TCP socket on 0.0.0.0:5000
  2. Set non-blocking mode, enable SO_REUSEADDR for rapid restarts
  3. Register the master socket with the selector (EVENT_READ)
  4. Enter the central while-True select loop
     • READ on master socket  → accept_handler   (new client arrives)
     • READ on client socket  → read_handler     (accumulate → parse → forward → relay)
  5. A SIGINT / SIGTERM handler tears down all open descriptors cleanly

Phase 4 additions
─────────────────
  • _health_monitor_loop()  — daemon thread waking every 3 s; probes each backend
                               port with a 1.0 s TCP timeout; flips healthy flags
                               under backend_pool["lock"] using fail/recover thresholds.
  • _pool_acquire()         — checks socket_pool[srv_id] for a warm idle socket;
                               peeks it to confirm liveness; falls back to fresh connect.
  • _pool_release()         — pushes the upstream socket back into socket_pool instead
                               of closing it (capped at SOCKET_POOL_MAX per server).
  • _forward_to_backend()   — updated to call _pool_acquire / _pool_release.

Verification (Phase 4 boundary):
  $ python mock_backends.py       # terminal 1
  $ python janus_core.py          # terminal 2
  # Kill one backend process — proxy must route around it within 3 s without 502.
  # Restart it — proxy must re-admit it automatically."""

import io
import json
import selectors
import signal
import socket
import sys
import time

# Force UTF-8 stdout on Windows (cp1252 default chokes on non-ASCII log chars).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

PROXY_HOST: str  = "0.0.0.0"
PROXY_PORT: int  = 5000
ADMIN_HOST: str  = "0.0.0.0"
ADMIN_PORT: int  = 5001   # Phase 6 — telemetry / management API port
SELECT_TIMEOUT: float = 1.0          # seconds — keeps the loop interruptible
RECV_CHUNK: int = 4096               # maximum bytes read per recv() call
MAX_HEADER_BUFFER: int = 8192        # TRD §5 — reject and 400 if no \r\n\r\n by this size

# ── Error responses: raw byte literals, no string-formatting libs ──────────
_BAD_REQUEST_400: bytes = (
    b"HTTP/1.1 400 Bad Request\r\n"
    b"Content-Type: text/plain\r\n"
    b"Content-Length: 11\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b"Bad Request"
)
_BAD_GATEWAY_502: bytes = (
    b"HTTP/1.1 502 Bad Gateway\r\n"
    b"Content-Type: text/plain\r\n"
    b"Content-Length: 11\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b"Bad Gateway"
)
_NOT_FOUND_404: bytes = (
    b"HTTP/1.1 404 Not Found\r\n"
    b"Content-Type: text/plain\r\n"
    b"Content-Length: 9\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b"Not Found"
)
_TOO_MANY_REQUESTS_429: bytes = (
    b"HTTP/1.1 429 Too Many Requests\r\n"
    b"Content-Type: text/plain\r\n"
    b"Content-Length: 19\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b"Rate Limit Exceeded"
)

# ──────────────────────────────────────────────────────────────────────────────
# IN-MEMORY STATE STRUCTURES
#
# Full schemas are defined in the TRD / in-memory-state docs.  All four
# structures are declared here at module level; later phases mutate them.
# ──────────────────────────────────────────────────────────────────────────────

# session_buffers  ── Dict[int, Dict]
#   Keyed by the client socket's file descriptor (fd integer).
#   Accumulates fragmented byte reads and tracks parsing state per connection.
#   Schema (TRD §3-D / in-memory-state §2-A):
#     {
#       fd: {
#         "client_ip"        : str,   # Dotted-quad source address
#         "raw_payload"      : bytes, # Accumulation buffer for arriving chunks
#         "headers_completed": bool,  # Flipped True once b"\r\n\r\n" is found
#         "bytes_expected"   : int,   # Content-Length value (POST bodies)
#         "backend_socket_fd": None   # Populated in Phase 3 when upstream assigned
#       }
#     }
session_buffers: dict = {}

# backend_pool  ── Dict[str, Any]
#   Single source of truth for round-robin distribution and backend health.
#   Schema (TRD §3-A / in-memory-state §2-B).
#   Health monitor (Phase 4) will write to this under backend_pool["lock"].
import threading
backend_pool: dict = {
    "active_index": 0,           # Round-Robin cursor (Phase 3 increments this)
    "servers": {
        "srv_01": {"host": "127.0.0.1", "port": 8001, "healthy": True, "consecutive_failures": 0},
        "srv_02": {"host": "127.0.0.1", "port": 8002, "healthy": True, "consecutive_failures": 0},
        "srv_03": {"host": "127.0.0.1", "port": 8003, "healthy": True, "consecutive_failures": 0},
    },
    "lock": threading.Lock(),    # Guards mutations from the health-monitor thread
}

# socket_pool  ── Dict[str, List[socket.socket]]
#   Idle, pre-warmed upstream sockets ready for immediate reuse (Phase 4).
#   Empty lists here = cold-start; Phase 3/4 will push recycled sockets in.
socket_pool: dict = {
    "srv_01": [],
    "srv_02": [],
    "srv_03": [],
}

# rate_limit_cache  ── Dict[str, Dict]
#   Token-bucket ledger keyed by client IP string (Phase 5).
#   Schema: { "1.2.3.4": {"tokens": float, "last_replenished": float} }
rate_limit_cache: dict = {}
BUCKET_MAX:   float = 30.0  # max tokens a single client IP can accumulate
REFILL_RATE:  float = 2.0   # tokens replenished per second (continuous drip)

# Mutex protecting rate_limit_cache reads and writes.
# Held only for pure arithmetic — never during network I/O — so hold
# time is microseconds and the event loop is never measurably stalled.
rate_limit_lock: threading.Lock = threading.Lock()

# Global telemetry counters — serialised to JSON on port 5001 in Phase 6.
_metrics: dict = {
    "total_requests_processed":   0,
    "aggregate_bytes_transferred": 0,
    "total_blocked_connections":   0,
    "start_time":                  time.time(),
}


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING UTILITY
# ──────────────────────────────────────────────────────────────────────────────

def _log(level: str, message: str) -> None:
    """
    Minimal synchronous stdout logger.

    Emits a line formatted as:
        [HH:MM:SS] [LEVEL] message

    Intentionally avoids the logging module's heavyweight configuration so
    output remains grep-friendly and CI-safe.
    """
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{level:<5}] {message}", flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 2 — HTTP HEADER PARSER
# ──────────────────────────────────────────────────────────────────────────────

# The delimiter that separates HTTP headers from the request body.
_HEADER_DELIM: bytes = b"\r\n\r\n"


def _parse_http_headers(raw: bytes) -> dict | None:
    """
    Parse the header section of a raw HTTP/1.1 byte stream.

    Contract
    ────────
    • `raw` must already contain the b"\r\n\r\n" boundary (caller's
      responsibility to check before calling).
    • Splits at the FIRST occurrence of b"\r\n\r\n" to isolate the header
      block from any body bytes.
    • Decodes the header block as UTF-8 (ASCII-safe for HTTP/1.1).
    • Returns a dict on success, None on any decode / structure error.

    Parsed fields (all present in the return dict, defaulting to "" / 0):
      method         — HTTP verb       (e.g. "GET", "POST", "OPTIONS")
      path           — Request target  (e.g. "/", "/api/v1/telemetry")
      version        — HTTP version    (e.g. "HTTP/1.1")
      content_length — int value of Content-Length header (0 if absent)
      connection     — value of Connection header ("" if absent)
      host           — value of Host header ("" if absent)
      raw_headers    — full decoded header section string (for debugging)

    No third-party helpers, no http.client, no urllib.  Pure byte / string ops.
    """
    try:
        header_bytes, _ = raw.split(_HEADER_DELIM, 1)
        header_text: str = header_bytes.decode("utf-8", errors="strict")
    except (ValueError, UnicodeDecodeError):
        return None

    lines = header_text.split("\r\n")
    if not lines:
        return None

    # ── Request line: METHOD SP Request-URI SP HTTP-Version ──────────────────
    request_line = lines[0]
    parts = request_line.split(" ", 2)
    if len(parts) != 3:
        return None  # Malformed request line.

    method, path, version = parts

    # Sanity-check the HTTP version token.
    if not version.startswith("HTTP/"):
        return None

    # ── Header fields (name: value pairs) ────────────────────────────────────
    content_length: int = 0
    connection: str = ""
    host: str = ""

    for line in lines[1:]:
        if ":" not in line:
            continue
        name, _, value = line.partition(":")
        name_lower = name.strip().lower()
        value_stripped = value.strip()

        if name_lower == "content-length":
            try:
                content_length = int(value_stripped)
            except ValueError:
                content_length = 0
        elif name_lower == "connection":
            connection = value_stripped.lower()
        elif name_lower == "host":
            host = value_stripped

    return {
        "method":         method.upper(),
        "path":           path,
        "version":        version,
        "content_length": content_length,
        "connection":     connection,
        "host":           host,
        "raw_headers":    header_text,
    }


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 3 — ROUND-ROBIN ROUTER & UPSTREAM TCP RELAY
# ──────────────────────────────────────────────────────────────────────────────

# Ordered list of server IDs — preserves a stable cycling sequence that the
# round-robin cursor can advance through with a simple modulo operation.
_SERVER_ORDER: list[str] = ["srv_01", "srv_02", "srv_03"]

# Upstream connect/receive timeout in seconds.
UPSTREAM_TIMEOUT: float = 5.0

# ── Phase 4 tuning constants ──────────────────────────────────────────────────
HEALTH_CHECK_INTERVAL:   float = 3.0  # seconds between heartbeat sweeps
HEALTH_PROBE_TIMEOUT:    float = 1.0  # TCP connect timeout per probe
HEALTH_FAIL_THRESHOLD:   int   = 3   # consecutive failures → mark unhealthy
HEALTH_RECOVER_THRESHOLD: int  = 2   # consecutive successes → restore healthy
SOCKET_POOL_MAX:          int   = 8   # max idle sockets kept per backend

# Dedicated lock for socket_pool mutations so health-monitor thread can
# drain a server's pool without racing the event-loop thread.
socket_pool_lock: threading.Lock = threading.Lock()


def _pick_backend() -> dict | None:
    """
    Atomically select the next healthy backend using Round-Robin logic.

    Algorithm
    ─────────
    1. Acquire backend_pool["lock"] so the Phase 4 health-monitor thread
       cannot mutate `is_healthy` flags mid-selection.
    2. Walk _SERVER_ORDER starting from `active_index` (wrapping with modulo).
    3. Return the first server dict whose `healthy` flag is True.
    4. Advance `active_index` past the chosen server so the *next* call picks
       the following one, guaranteeing strict sequential distribution.
    5. If all servers are unhealthy, return None.

    The lock is held only for the cursor read/write — not during the actual
    network I/O — so it never stalls the event loop for long.
    """
    servers = backend_pool["servers"]
    n       = len(_SERVER_ORDER)

    with backend_pool["lock"]:
        start = backend_pool["active_index"]

        for offset in range(n):
            idx       = (start + offset) % n
            server_id = _SERVER_ORDER[idx]
            srv       = servers[server_id]

            if srv["healthy"]:
                # Advance the cursor PAST the chosen server so the next call
                # picks the one after it, not the same one again.
                backend_pool["active_index"] = (idx + 1) % n
                return {"id": server_id, **srv}

    # All servers are currently marked unhealthy.
    return None


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 4-A — BACKGROUND HEALTH MONITOR DAEMON
# ──────────────────────────────────────────────────────────────────────────────

def _health_monitor_loop() -> None:
    """
    Runs inside a daemon threading.Thread started from main().

    Every HEALTH_CHECK_INTERVAL seconds it iterates over every entry in
    backend_pool["servers"] and attempts a TCP connect to its (host, port).

    Failure path:
      • A failed probe increments server["consecutive_failures"].
      • Once consecutive_failures >= HEALTH_FAIL_THRESHOLD the server is
        marked healthy=False under backend_pool["lock"].
      • All pooled sockets for that server are drained and closed immediately
        so the event loop never tries to reuse a socket to a dead backend.

    Recovery path:
      • A successful probe decrements consecutive_failures (floor 0) and
        increments a transient "consecutive_successes" counter.
      • Once consecutive_successes >= HEALTH_RECOVER_THRESHOLD the server is
        restored to healthy=True and consecutive_failures is reset to 0.

    Lock discipline:
      • backend_pool["lock"] is held ONLY while reading/writing the health
        flags — never during the network probe itself.  This means the lock
        is held for microseconds, not milliseconds, so the event loop is
        never blocked waiting for a slow probe.
    """
    # Per-server transient success counter lives only in this thread.
    recovery_counters: dict[str, int] = {sid: 0 for sid in _SERVER_ORDER}

    while _running:
        time.sleep(HEALTH_CHECK_INTERVAL)
        if not _running:
            break

        for server_id in _SERVER_ORDER:
            # ── Read current config under lock ──────────────────────────────
            with backend_pool["lock"]:
                srv  = backend_pool["servers"][server_id]
                host = srv["host"]
                port = srv["port"]
                was_healthy = srv["healthy"]

            # ── Probe: attempt TCP handshake with a short timeout ────────────
            probe_ok = False
            try:
                probe_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                probe_sock.settimeout(HEALTH_PROBE_TIMEOUT)
                probe_sock.connect((host, port))
                probe_sock.close()
                probe_ok = True
            except OSError:
                try:
                    probe_sock.close()
                except OSError:
                    pass

            # ── Update state under lock ──────────────────────────────────────
            with backend_pool["lock"]:
                srv = backend_pool["servers"][server_id]

                if probe_ok:
                    # Successful probe: credit a recovery tick.
                    srv["consecutive_failures"] = max(0, srv["consecutive_failures"] - 1)
                    recovery_counters[server_id] += 1

                    if (not was_healthy
                            and recovery_counters[server_id] >= HEALTH_RECOVER_THRESHOLD):
                        srv["healthy"] = True
                        srv["consecutive_failures"] = 0
                        recovery_counters[server_id] = 0
                        _log("INFO ",
                             f"[HEALTH  ] {server_id} RECOVERED — restored to routing pool")
                    elif not was_healthy:
                        _log("DEBUG",
                             f"[HEALTH  ] {server_id} probing recovery "
                             f"({recovery_counters[server_id]}/{HEALTH_RECOVER_THRESHOLD})")
                else:
                    # Failed probe.
                    recovery_counters[server_id] = 0
                    srv["consecutive_failures"] += 1
                    fails = srv["consecutive_failures"]

                    if was_healthy and fails >= HEALTH_FAIL_THRESHOLD:
                        srv["healthy"] = False
                        _log("WARN ",
                             f"[HEALTH  ] {server_id} OFFLINE after "
                             f"{fails} consecutive failures — removed from pool")
                        # Drain pooled sockets for this server immediately.
                        _drain_pool(server_id)
                    elif was_healthy:
                        _log("DEBUG",
                             f"[HEALTH  ] {server_id} probe fail "
                             f"{fails}/{HEALTH_FAIL_THRESHOLD}")


def _drain_pool(server_id: str) -> None:
    """
    Close and discard every idle socket in socket_pool[server_id].
    Called by the health monitor when a server goes offline so the event
    loop never pops a stale socket to a dead backend.
    """
    with socket_pool_lock:
        stale = socket_pool.get(server_id, [])
        socket_pool[server_id] = []

    for sock in stale:
        try:
            sock.close()
        except OSError:
            pass

    if stale:
        _log("INFO ",
             f"[POOL    ] drained {len(stale)} stale socket(s) for {server_id}")


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 4-B — CONNECTION POOL ACQUIRE / RELEASE
# ──────────────────────────────────────────────────────────────────────────────

def _pool_acquire(server_id: str, host: str, port: int,
                  client_fd: int) -> socket.socket | None:
    """
    Return a live, writable socket connected to (host, port).

    Strategy
    ────────
    1. Pop from socket_pool[server_id] if any idle sockets are available.
    2. Validate the popped socket with a non-blocking zero-byte recv() peek:
         • EAGAIN/BlockingIOError  → socket is alive (no data, no close)  ✓
         • 0-byte recv             → server closed its end quietly        ✗ discard
         • Any other OSError       → socket is broken                     ✗ discard
    3. If validation fails, try the next pooled socket.  Continue until
       the pool is empty or a good socket is found.
    4. If pool is exhausted, open a fresh blocking TCP connection.
    5. Return None on fresh-connect failure (caller will send 502).
    """
    # ── Step 1 & 2: Try pooled sockets ──────────────────────────────────────
    while True:
        with socket_pool_lock:
            pool = socket_pool.get(server_id, [])
            if not pool:
                break
            candidate = pool.pop()

        # Peek: set non-blocking temporarily, recv 1 byte with MSG_PEEK.
        alive = False
        try:
            candidate.setblocking(False)
            data = candidate.recv(1, socket.MSG_PEEK)
            if data:
                # There's unread data on the socket — still connected.
                alive = True
            # data == b"" means server sent FIN → dead, fall through.
        except BlockingIOError:
            # EAGAIN: no data waiting, socket is healthy.
            alive = True
        except OSError:
            alive = False
        finally:
            try:
                candidate.setblocking(True)
                candidate.settimeout(UPSTREAM_TIMEOUT)
            except OSError:
                alive = False

        if alive:
            _log("DEBUG",
                 f"[POOL    ] fd={client_fd:<6} reused warm socket "
                 f"for {server_id} ({host}:{port})")
            return candidate
        else:
            _log("DEBUG",
                 f"[POOL    ] fd={client_fd:<6} discarded dead socket "
                 f"for {server_id}")
            try:
                candidate.close()
            except OSError:
                pass

    # ── Step 3: Fresh connect ────────────────────────────────────────────────
    try:
        fresh = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        fresh.settimeout(UPSTREAM_TIMEOUT)
        fresh.connect((host, port))
        _log("DEBUG",
             f"[POOL    ] fd={client_fd:<6} opened fresh socket "
             f"to {server_id} ({host}:{port})")
        return fresh
    except OSError as exc:
        _log("WARN ",
             f"[UPSTREAM] fd={client_fd:<6} {server_id} fresh connect failed — {exc}")
        try:
            fresh.close()
        except OSError:
            pass
        return None


def _pool_release(server_id: str, sock: socket.socket) -> None:
    """
    Return a completed upstream socket to the idle pool for reuse.

    If the pool for this server is already at SOCKET_POOL_MAX, close the
    socket immediately rather than letting the pool grow unboundedly.
    The pool is also checked by the health monitor, so we hold
    socket_pool_lock for the brief append/check operation.
    """
    with socket_pool_lock:
        pool = socket_pool.setdefault(server_id, [])
        if len(pool) < SOCKET_POOL_MAX:
            pool.append(sock)
            return

    # Pool is full — discard.
    try:
        sock.close()
    except OSError:
        pass


def _forward_to_backend(
    client_sock: socket.socket,
    client_fd:   int,
    raw_payload: bytes,
    server:      dict,
) -> None:
    """
    Acquire an upstream socket (pooled or fresh), forward raw_payload,
    stream the full response back to client_sock, then recycle the
    upstream socket into the pool (Phase 4).

    Error policy:
      • If _pool_acquire() returns None, send 502 and return.
      • On sendall failure: close upstream (don't recycle broken socket), 502.
      • On recv failure mid-stream: close upstream, log, stop relay.
      • On success: call _pool_release() so next request skips the handshake.
    """
    host   = server["host"]
    port   = server["port"]
    srv_id = server["id"]

    # ── Acquire: pooled warm socket or fresh connect ─────────────────────────
    upstream_sock = _pool_acquire(srv_id, host, port, client_fd)
    if upstream_sock is None:
        try:
            client_sock.sendall(_BAD_GATEWAY_502)
        except OSError:
            pass
        return

    _log("INFO ",
         f"[ROUTE   ] fd={client_fd:<6} -> {srv_id}  "
         f"({host}:{port})  payload={len(raw_payload)} bytes")

    # ── Forward the exact client byte payload ──────────────────────────────
    try:
        upstream_sock.sendall(raw_payload)
    except OSError as exc:
        _log("WARN ", f"[UPSTREAM] fd={client_fd:<6} {srv_id} send failed — {exc}")
        try:
            client_sock.sendall(_BAD_GATEWAY_502)
        except OSError:
            pass
        try:
            upstream_sock.close()      # broken — discard, don't recycle
        except OSError:
            pass
        return

    # ── Stream response from upstream back to client ────────────────────────
    total_relayed = 0
    relay_start   = time.perf_counter()
    relay_ok      = True

    while True:
        try:
            chunk = upstream_sock.recv(RECV_CHUNK)
        except OSError as exc:
            _log("WARN ", f"[UPSTREAM] fd={client_fd:<6} {srv_id} recv failed — {exc}")
            relay_ok = False
            break

        if not chunk:
            break                       # server closed connection cleanly

        try:
            client_sock.sendall(chunk)
        except OSError as exc:
            _log("WARN ", f"[RELAY   ] fd={client_fd:<6} client send failed — {exc}")
            relay_ok = False
            break

        total_relayed += len(chunk)

    latency_ms = (time.perf_counter() - relay_start) * 1000
    _metrics["aggregate_bytes_transferred"] += total_relayed

    _log("INFO ",
         f"[RELAY   ] fd={client_fd:<6} <- {srv_id}  "
         f"{total_relayed} bytes  latency={latency_ms:.2f}ms  "
         f"req_total={_metrics['total_requests_processed']}")

    # ── Release: recycle on success, close on error ─────────────────────────
    if relay_ok:
        _pool_release(srv_id, upstream_sock)
        pool_size = len(socket_pool.get(srv_id, []))
        _log("DEBUG",
             f"[POOL    ] {srv_id} pool_size={pool_size} after release")
    else:
        try:
            upstream_sock.close()
        except OSError:
            pass



def accept_handler(master_sock: socket.socket, mask: int, sel: selectors.BaseSelector) -> None:
    """
    Called by the event loop whenever the master socket becomes readable,
    meaning the OS has completed a TCP 3-way handshake with a new client.

    Phase 5 execution order (Bouncer gate is the FIRST thing that runs):
      1. accept()            — extract raw socket + client IP.
      2. TOKEN-BUCKET CHECK  — evaluate rate_limit_cache[client_ip].
                               BLOCK  → send 429, close socket, return.
                               PASS   → continue below.
      3. setblocking(False)  — mandatory for the selector model.
      4. session_buffers allocation.
      5. sel.register()      — client enters the event loop.
    """
    try:
        client_sock, client_addr = master_sock.accept()
    except OSError as exc:
        # Can happen if the OS exhausts its file descriptor table.
        _log("ERROR", f"accept() failed: {exc}")
        return

    client_ip: str = client_addr[0]
    client_fd: int = client_sock.fileno()

    # ── Phase 5: Token-bucket gate ────────────────────────────────────────────
    # Runs immediately after accept() — before setblocking, session alloc,
    # or selector registration.  Blocked clients consume zero server resources.
    now: float = time.monotonic()  # monotonic: immune to wall-clock adjustments

    with rate_limit_lock:
        entry = rate_limit_cache.get(client_ip)

        if entry is None:
            # First request from this IP — initialise a full bucket.
            rate_limit_cache[client_ip] = {
                "tokens":           BUCKET_MAX,
                "last_replenished": now,
            }
            entry = rate_limit_cache[client_ip]

        else:
            # Lazy replenishment: credit tokens earned since last visit.
            elapsed = now - entry["last_replenished"]
            entry["tokens"] = min(
                BUCKET_MAX,
                entry["tokens"] + elapsed * REFILL_RATE,
            )
            entry["last_replenished"] = now

        # ── Gatekeeper decision ────────────────────────────────────────
        if entry["tokens"] >= 1.0:
            entry["tokens"] -= 1.0      # admit: spend one token
            admitted = True
        else:
            admitted = False
            current_tokens = entry["tokens"]   # snapshot before releasing lock

    # Lock released before any I/O — hold time was pure arithmetic only.

    if not admitted:
        # ── BLOCKED ──
        _metrics["total_blocked_connections"] += 1
        _log("WARN ",
             f"[BOUNCER ] fd={client_fd:<6} src={client_ip:<18} "
             f"Dropped due to rate limit (Tokens: {current_tokens:.2f})")
        try:
            client_sock.sendall(_TOO_MANY_REQUESTS_429)
        except OSError:
            pass
        try:
            client_sock.close()
        except OSError:
            pass
        return  # never reaches selector registration

    # ── ADMITTED ──
    # Enforce non-blocking I/O — mandatory for the selector model.
    client_sock.setblocking(False)

    # Initialise the session record for this descriptor.
    session_buffers[client_fd] = {
        "client_ip":         client_ip,
        "raw_payload":       b"",
        "headers_completed": False,
        "bytes_expected":    0,
        "backend_socket_fd": None,
    }

    # Register for reads; bind the dispatcher so the loop stays generic.
    sel.register(
        client_sock,
        selectors.EVENT_READ,
        data=lambda sock, m: read_handler(sock, m, sel),
    )

    _log("INFO ", f"[CONNECT ] fd={client_fd:<6} src={client_ip}  — connection accepted")


def read_handler(client_sock: socket.socket, mask: int, sel: selectors.BaseSelector) -> None:
    """
    Called by the event loop when a registered client socket has bytes ready
    to read from the OS receive buffer.

    Phase 2 contract:
      • Drain up to RECV_CHUNK bytes per selector wake-up.
      • Append chunk to session_buffers[fd]["raw_payload"] (fragment assembly).
      • If recv() returns 0 bytes the peer has closed; tear down the fd.
      • Scan the accumulation buffer for b"\r\n\r\n":
          – Not found yet  → return and wait for the selector's next wake-up.
          – Buffer exceeds MAX_HEADER_BUFFER with no delimiter → 400 + close.
          – Found → call _parse_http_headers(); log parsed fields.
      • On successful parse, send the Phase 2 hardcoded 200 mock response,
        flip session_buffers[fd]["headers_completed"] = True, then close.
        (Phase 3 will replace the mock response with real upstream forwarding.)

    BlockingIOError / EAGAIN:
      Non-blocking recv() raises BlockingIOError when the OS receive buffer is
      momentarily empty.  We return immediately; the selector will wake us
      again when data arrives — no spin, no block.
    """
    client_fd: int = client_sock.fileno()

    # ── Guard: discard callbacks for already-cleaned-up fds ─────────────────
    if client_fd not in session_buffers:
        return

    session = session_buffers[client_fd]

    # ── Step 1: Drain the OS receive buffer ──────────────────────────────────
    try:
        chunk: bytes = client_sock.recv(RECV_CHUNK)
    except BlockingIOError:
        # Kernel buffer temporarily empty — perfectly normal for non-blocking.
        return
    except OSError as exc:
        _log("WARN ", f"[RECV ERR] fd={client_fd}  — {exc}")
        _teardown_client(client_sock, sel)
        return

    if not chunk:
        # Zero-byte read: peer sent FIN, connection is half-closed.
        _log("INFO ", f"[DISCONN ] fd={client_fd:<6} — peer closed connection (0-byte read)")
        _teardown_client(client_sock, sel)
        return

    # ── Step 2: Accumulate into the session's fragment buffer ────────────────
    session["raw_payload"] += chunk
    buf: bytes = session["raw_payload"]
    total: int = len(buf)

    _log("DEBUG", f"[RECV    ] fd={client_fd:<6} +{len(chunk):>5} bytes  "
                  f"buffered={total:>6} bytes")

    # ── Step 3: Check for b"\r\n\r\n" header-end delimiter ───────────────────
    if _HEADER_DELIM not in buf:
        # Headers not yet complete — check buffer overflow guard.
        if total > MAX_HEADER_BUFFER:
            _log("WARN ", f"[400     ] fd={client_fd:<6} — buffer overflow "
                          f"({total} bytes, no delimiter found) → 400 Bad Request")
            try:
                client_sock.sendall(_BAD_REQUEST_400)
            except OSError:
                pass
            _teardown_client(client_sock, sel)
        # Either way, return and wait for more data.
        return

    # ── Step 4: Headers complete — parse them ────────────────────────────────
    parsed = _parse_http_headers(buf)

    if parsed is None:
        # Buffer contained the delimiter but header structure was malformed.
        _log("WARN ", f"[400     ] fd={client_fd:<6} — malformed HTTP headers → 400 Bad Request")
        try:
            client_sock.sendall(_BAD_REQUEST_400)
        except OSError:
            pass
        _teardown_client(client_sock, sel)
        return

    # ── Step 5: Log the parsed metadata (the Phase 2 verification output) ────
    _log("INFO ",
         f"[PARSE   ] fd={client_fd:<6} "
         f"Method={parsed['method']:<8} "
         f"Path={parsed['path']:<30} "
         f"Version={parsed['version']}")
    _log("INFO ",
         f"[PARSE   ] fd={client_fd:<6} "
         f"Host={parsed['host'] or '(none)':<25} "
         f"Connection={parsed['connection'] or '(none)':<12} "
         f"Content-Length={parsed['content_length']}")

    # ── Step 6: Update the session state ─────────────────────────────────────
    session["headers_completed"] = True
    session["bytes_expected"]    = parsed["content_length"]

    # Bump the global processed-requests counter (Phase 6 telemetry).
    _metrics["total_requests_processed"] += 1

    # ── Step 7: Round-Robin — pick the next healthy backend ──────────────────
    server = _pick_backend()

    if server is None:
        # Every backend is currently offline — return 502 and close.
        _log("WARN ", f"[NO BKND ] fd={client_fd:<6} — all backends unhealthy -> 502")
        try:
            client_sock.sendall(_BAD_GATEWAY_502)
        except OSError:
            pass
        _teardown_client(client_sock, sel)
        return

    # ── Step 8: Forward payload upstream and relay response to client ─────────
    # Pass the full accumulated raw_payload (headers + any body bytes) so the
    # backend receives a well-formed HTTP request.
    _forward_to_backend(
        client_sock=client_sock,
        client_fd=client_fd,
        raw_payload=session["raw_payload"],
        server=server,
    )

    # Phase 3: close client after each transaction (Phase 4 adds keep-alive).
    _teardown_client(client_sock, sel)


def _teardown_client(client_sock: socket.socket, sel: selectors.BaseSelector) -> None:
    """
    Unregister a client descriptor from the selector and release OS resources.

    Called on:
      • Clean EOF   (recv returns 0 bytes)
      • Socket error (broken pipe, reset, etc.)
      • Future phases: post-response cleanup, 400/429 dispatch

    Order matters: unregister before close() so the selector does not hold
    a stale reference to a closed fd.
    """
    client_fd: int = client_sock.fileno()

    try:
        sel.unregister(client_sock)
    except KeyError:
        # Already unregistered — safe to ignore.
        pass

    try:
        client_sock.close()
    except OSError:
        pass

    # Evict the session record to free memory.
    session_buffers.pop(client_fd, None)
    _log("INFO ", f"[CLEANUP ] fd={client_fd:<6} — descriptor released")


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 6 — ADMIN TELEMETRY API  (port 5001)
# ──────────────────────────────────────────────────────────────────────────────

def _build_admin_socket() -> socket.socket:
    """
    Allocate the administrative TCP socket bound to ADMIN_PORT (5001).
    Identical setup to the proxy master socket: SO_REUSEADDR + non-blocking.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((ADMIN_HOST, ADMIN_PORT))
    sock.listen(32)
    sock.setblocking(False)
    return sock


def _serve_metrics(admin_client: socket.socket) -> None:
    """
    Compile a live JSON telemetry snapshot and write it to `admin_client`.

    All shared-state reads are wrapped in their respective mutex locks so the
    response is always internally consistent despite concurrent health-monitor
    and event-loop mutations:

      backend_pool["lock"]  — guards backend healthy/failure flags
      socket_pool_lock      — guards pool list lengths
      rate_limit_lock       — guards rate_limit_cache iteration

    The JSON schema:
    {
      "uptime_seconds": float,
      "metrics": {
        "total_requests_processed":   int,
        "aggregate_bytes_transferred": int,
        "total_blocked_connections":  int,
      },
      "backends": {
        "srv_01": {"host": str, "port": int, "healthy": bool,
                   "consecutive_failures": int},
        ...
      },
      "pools": {
        "srv_01": int,   # warm idle sockets available
        ...
      },
      "rate_limits": {
        "tracked_ips": int
      }
    }
    """
    now = time.time()

    # ── Snapshot backend state under lock ────────────────────────────────────
    with backend_pool["lock"]:
        backends_snap = {
            sid: {
                "host":                 srv["host"],
                "port":                 srv["port"],
                "healthy":              srv["healthy"],
                "consecutive_failures": srv["consecutive_failures"],
            }
            for sid, srv in backend_pool["servers"].items()
        }

    # ── Snapshot pool depths under lock ──────────────────────────────────────
    with socket_pool_lock:
        pools_snap = {sid: len(lst) for sid, lst in socket_pool.items()}

    # ── Count tracked IPs under lock ─────────────────────────────────────────
    with rate_limit_lock:
        tracked_ips = len(rate_limit_cache)

    payload = {
        "uptime_seconds": round(now - _metrics["start_time"], 2),
        "metrics": {
            "total_requests_processed":    _metrics["total_requests_processed"],
            "aggregate_bytes_transferred": _metrics["aggregate_bytes_transferred"],
            "total_blocked_connections":   _metrics["total_blocked_connections"],
        },
        "backends": backends_snap,
        "pools":    pools_snap,
        "rate_limits": {
            "tracked_ips": tracked_ips,
        },
    }

    body    = json.dumps(payload, indent=2).encode("utf-8")
    headers = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Access-Control-Allow-Origin: *\r\n"
        + f"Content-Length: {len(body)}\r\n".encode()
        + b"Connection: close\r\n"
        b"\r\n"
    )
    try:
        admin_client.sendall(headers + body)
    except OSError:
        pass


def admin_accept_handler(
    admin_master: socket.socket,
    mask: int,
    sel: selectors.BaseSelector,
) -> None:
    """
    Accept one admin connection, read the request line, dispatch to
    _serve_metrics() if the path is /api/v1/metrics, else return 404.

    Admin connections are ephemeral: serve one request, then immediately
    close the socket.  No session buffer, no selector registration needed
    — the admin port is deliberately simple and stateless.
    """
    try:
        client, addr = admin_master.accept()
    except OSError:
        return

    client.settimeout(2.0)   # admin client gets a 2-second read deadline
    try:
        raw = client.recv(512)
    except OSError:
        raw = b""
    finally:
        client.settimeout(None)

    # Parse only the request line — we only need the path.
    path = "/"
    if raw:
        first_line = raw.split(b"\r\n")[0].decode(errors="replace")
        parts = first_line.split()
        if len(parts) >= 2:
            path = parts[1]

    if path == "/api/v1/metrics":
        _serve_metrics(client)
        _log("DEBUG", f"[ADMIN   ] {addr[0]} GET /api/v1/metrics — served")
    else:
        try:
            client.sendall(_NOT_FOUND_404)
        except OSError:
            pass
        _log("DEBUG", f"[ADMIN   ] {addr[0]} {path} — 404")

    try:
        client.close()
    except OSError:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# ──────────────────────────────────────────────────────────────────────────────

# Module-level flag checked by the event loop.
_running: bool = True


def _shutdown_handler(signum: int, frame) -> None:  # noqa: ANN001
    """
    SIGINT / SIGTERM handler.  Flips _running to False so the select loop
    exits cleanly on the next timeout tick rather than being killed mid-recv().
    """
    global _running
    signal.signal(signum, signal.SIG_DFL)   # Re-arm default so a second signal kills hard.
    _log("INFO ", f"Signal {signum} received — initiating graceful shutdown …")
    _running = False


# ──────────────────────────────────────────────────────────────────────────────
# MASTER SOCKET SETUP
# ──────────────────────────────────────────────────────────────────────────────

def _build_master_socket() -> socket.socket:
    """
    Allocate and configure the inbound-facing proxy socket.

    Settings applied:
      • AF_INET / SOCK_STREAM  — standard IPv4 TCP
      • SO_REUSEADDR           — allow immediate rebind after SIGTERM (avoids
                                 TIME_WAIT port lock during development)
      • setblocking(False)     — mandatory for the selectors model; ensures
                                 accept() never stalls the event loop
      • listen(128)            — OS backlog queue depth; 128 is a sensible
                                 default for a local development proxy
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((PROXY_HOST, PROXY_PORT))
    sock.listen(128)
    sock.setblocking(False)
    return sock


# ──────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT — THE SELECT EVENT LOOP
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Bootstrap Janus and run the non-blocking selector event loop.

    Loop anatomy:
      selector.select(timeout=SELECT_TIMEOUT)
        Returns a list of (SelectorKey, events) pairs for all file descriptors
        that became ready within the timeout window.  When the list is empty
        the proxy simply loops back — this is the idle heartbeat tick.

      key.data(key.fileobj, mask)
        Dispatches to the handler function stored at registration time:
          • master_sock  → accept_handler  (wrapped lambda carrying sel ref)
          • client_sock  → read_handler    (wrapped lambda carrying sel ref)

    The timeout-based loop means SIGINT unblocks within ≤ SELECT_TIMEOUT
    seconds, giving a deterministic shutdown window.
    """
    global _running

    # Wire up clean shutdown signals before touching the network.
    signal.signal(signal.SIGINT,  _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    master_sock = _build_master_socket()
    admin_sock  = _build_admin_socket()
    sel = selectors.DefaultSelector()

    # Register the proxy master socket.
    sel.register(
        master_sock,
        selectors.EVENT_READ,
        data=lambda sock, mask: accept_handler(sock, mask, sel),
    )

    # Register the admin master socket on port 5001.
    sel.register(
        admin_sock,
        selectors.EVENT_READ,
        data=lambda sock, mask: admin_accept_handler(sock, mask, sel),
    )

    _log("INFO ", "=" * 62)
    _log("INFO ", "  JANUS  —  Layer 7 Load Balancer  [Phase 6 Boot]")
    _log("INFO ", "=" * 62)
    _log("INFO ", f"  Proxy port      {PROXY_HOST}:{PROXY_PORT}")
    _log("INFO ", f"  Admin API port  {ADMIN_HOST}:{ADMIN_PORT}  (GET /api/v1/metrics)")
    _log("INFO ", f"  Selector        {type(sel).__name__}")
    _log("INFO ", f"  Recv chunk      {RECV_CHUNK} bytes")
    _log("INFO ", f"  Max hdr buffer  {MAX_HEADER_BUFFER} bytes")
    _log("INFO ", f"  Upstream tmout  {UPSTREAM_TIMEOUT}s")
    _log("INFO ", f"  Health interval {HEALTH_CHECK_INTERVAL}s  "
                  f"fail@{HEALTH_FAIL_THRESHOLD}  recover@{HEALTH_RECOVER_THRESHOLD}")
    _log("INFO ", f"  Pool max        {SOCKET_POOL_MAX} sockets/backend")
    _log("INFO ", f"  Bouncer Mode    capacity={BUCKET_MAX} tok  refill={REFILL_RATE} tok/s")
    _log("INFO ", "  Backends        srv_01:8001  srv_02:8002  srv_03:8003")
    _log("INFO ", "  Mode            Phase 6 — Telemetry API + CRT Dashboard")
    _log("INFO ", "=" * 62)
    _log("INFO ", "  Press Ctrl+C to stop.")
    _log("INFO ", "")

    # ── Spawn the background health-monitor daemon ────────────────────────────
    health_thread = threading.Thread(
        target=_health_monitor_loop,
        name="janus-health-monitor",
        daemon=True,         # dies automatically when the main process exits
    )
    health_thread.start()
    _log("INFO ", f"  Health monitor started (tid={health_thread.ident})")
    _log("INFO ", "")

    try:
        while _running:
            # Block until at least one fd is ready, or the timeout elapses.
            # timeout=SELECT_TIMEOUT means the loop checks _running ~every second.
            ready_events = sel.select(timeout=SELECT_TIMEOUT)

            for key, mask in ready_events:
                # key.data holds the pre-bound handler lambda registered above.
                callback = key.data
                try:
                    callback(key.fileobj, mask)
                except Exception as exc:  # noqa: BLE001
                    # Isolate per-connection errors — one bad socket must never
                    # crash the entire event loop.
                    fd = key.fileobj.fileno() if key.fileobj else "?"
                    _log("ERROR", f"Unhandled exception on fd={fd}: {exc}")
                    try:
                        _teardown_client(key.fileobj, sel)
                    except Exception:
                        pass

    finally:
        # ── Graceful teardown ───────────────────────────────────────────────
        _log("INFO ", "")
        _log("INFO ", "Closing all open descriptors …")

        # Close every registered client socket before the masters.
        for key in list(sel.get_map().values()):
            if key.fileobj not in (master_sock, admin_sock):
                try:
                    sel.unregister(key.fileobj)
                    key.fileobj.close()
                except OSError:
                    pass

        sel.close()
        master_sock.close()
        admin_sock.close()

        open_sessions = len(session_buffers)
        if open_sessions:
            _log("WARN ", f"{open_sessions} session buffer(s) still in memory — evicting")
            session_buffers.clear()

        _log("INFO ", "Janus stopped cleanly.  Goodbye.")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
