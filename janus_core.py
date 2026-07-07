"""
janus_core.py
=============
Project : Janus — Custom Layer 7 Load Balancer
Phase   : 2 — Byte-Stream Accumulation & HTTP Request Parsing

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
     • READ on client socket  → read_handler     (accumulate → parse → respond)
  5. A SIGINT / SIGTERM handler tears down all open descriptors cleanly

Phase 2 additions
─────────────────
  • Byte accumulation into session_buffers[fd]["raw_payload"] across recv() calls
  • b"\r\n\r\n" boundary scanning — handler returns early if headers are incomplete
  • Manual HTTP/1.1 header parsing: method, path, version, Content-Length, Connection
  • 400 Bad Request on malformed input or buffer overflow (> MAX_HEADER_BUFFER bytes)
  • Hardcoded 200 OK mock response so curl/browser closes cleanly (no forwarding yet)

Verification (Phase 2 boundary):
  $ python janus_core.py
  $ curl -v http://localhost:5000/          # → logs Method=GET Path=/ Connection=keep-alive
  $ curl -v -X POST http://localhost:5000/data -d "hello"  # → logs Content-Length=5
"""

import io
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

PROXY_HOST: str = "0.0.0.0"
PROXY_PORT: int = 5000
SELECT_TIMEOUT: float = 1.0          # seconds — keeps the loop interruptible
RECV_CHUNK: int = 4096               # maximum bytes read per recv() call
MAX_HEADER_BUFFER: int = 8192        # TRD §5 — reject and 400 if no \r\n\r\n by this size

# ── Phase 2: hardcoded mock response (replaced by real upstream relay in Phase 3) ──
_MOCK_200: bytes = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Type: text/plain\r\n"
    b"Content-Length: 18\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b"Janus Parser Alive"
)
_BAD_REQUEST_400: bytes = (
    b"HTTP/1.1 400 Bad Request\r\n"
    b"Content-Type: text/plain\r\n"
    b"Content-Length: 11\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b"Bad Request"
)

# ──────────────────────────────────────────────────────────────────────────────
# IN-MEMORY STATE STRUCTURES  (Phase 1 subset — only what Phase 1 touches)
#
# Full schemas are defined in the TRD / in-memory-state docs.  Later phases
# will populate and mutate these; Phase 1 merely declares and owns them.
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
#   Config constants: BUCKET_MAX = 30.0 tokens, REFILL_RATE = 2.0 tokens/sec.
rate_limit_cache: dict = {}
BUCKET_MAX:   float = 30.0
REFILL_RATE:  float = 2.0   # tokens per second

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
# PHASE 1 CALLBACKS
# ──────────────────────────────────────────────────────────────────────────────

def accept_handler(master_sock: socket.socket, mask: int, sel: selectors.BaseSelector) -> None:
    """
    Called by the event loop whenever the master socket becomes readable,
    meaning the OS has completed a TCP 3-way handshake with a new client.

    Responsibilities (Phase 1):
      1. Accept the connection — extract the raw client socket and address.
      2. Set the client socket to non-blocking mode immediately.
      3. Allocate a fresh session_buffers entry keyed by the client's fd.
      4. Register the client socket with the selector for EVENT_READ.
         The attached data object is the read_handler callback so the loop
         can dispatch uniformly via  key.data(key.fileobj, mask).
    """
    try:
        client_sock, client_addr = master_sock.accept()
    except OSError as exc:
        # Can happen if the OS exhausts its file descriptor table.
        _log("ERROR", f"accept() failed: {exc}")
        return

    client_ip: str = client_addr[0]
    client_fd: int = client_sock.fileno()

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

    # ── Step 7: Send Phase 2 mock response & close ───────────────────────────
    # Phase 3 will replace this block with real upstream forwarding.
    try:
        client_sock.sendall(_MOCK_200)
    except OSError as exc:
        _log("WARN ", f"[SEND ERR] fd={client_fd}  — {exc}")

    _log("INFO ",
         f"[RESPOND ] fd={client_fd:<6} "
         f"-> 200 OK  (mock, {len(_MOCK_200)} bytes)  "
         f"req_total={_metrics['total_requests_processed']}")

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
    sel = selectors.DefaultSelector()

    # Register the master socket.  The callback is a lambda that injects the
    # selector reference so accept_handler can register new client sockets.
    sel.register(
        master_sock,
        selectors.EVENT_READ,
        data=lambda sock, mask: accept_handler(sock, mask, sel),
    )

    _log("INFO ", "=" * 60)
    _log("INFO ", " JANUS  —  Layer 7 Load Balancer  [Phase 2 Boot]")
    _log("INFO ", "=" * 60)
    _log("INFO ", f" Listening on    {PROXY_HOST}:{PROXY_PORT}")
    _log("INFO ", f" Selector        {type(sel).__name__}")
    _log("INFO ", f" Select tick     {SELECT_TIMEOUT}s timeout")
    _log("INFO ", f" Recv chunk      {RECV_CHUNK} bytes")
    _log("INFO ", f" Max hdr buffer  {MAX_HEADER_BUFFER} bytes")
    _log("INFO ", " Backends        srv_01:8001  srv_02:8002  srv_03:8003")
    _log("INFO ", " Mode            Phase 2 — accumulate + parse + mock-respond")
    _log("INFO ", "=" * 60)
    _log("INFO ", " Press Ctrl+C to stop.")
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

        # Close every registered client socket before the master.
        for key in list(sel.get_map().values()):
            if key.fileobj is not master_sock:
                try:
                    sel.unregister(key.fileobj)
                    key.fileobj.close()
                except OSError:
                    pass

        sel.close()
        master_sock.close()

        open_sessions = len(session_buffers)
        if open_sessions:
            _log("WARN ", f"{open_sessions} session buffer(s) still in memory — evicting")
            session_buffers.clear()

        _log("INFO ", "Janus stopped cleanly.  Goodbye.")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
