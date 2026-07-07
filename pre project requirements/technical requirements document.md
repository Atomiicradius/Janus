# File 2: Technical Requirements Document (TRD) — Refined Version

## Project Name: Janus

## Project Title: Custom Layer 7 Load Balancer with Byte-Stream Parsing & TCP Connection Pooling

---

## 1. Core Technology Stack

* 
**Proxy Core Engine:** Python 3.10+ (Standard Library exclusively).


* 
**Mock Backend Fleet:** Python with Flask (isolated, target mock endpoints).


* 
**Telemetry Admin Dashboard:** React (Vite), JavaScript/TypeScript, Chart.js.



---

## 2. Definitive Architectural Constraints

To maintain systems-level integrity and eliminate AI generation shortcuts, the agent must adhere to these absolute rules:

* 
**No High-Level Network Libraries:** The use of `FastAPI`, `Flask` (for the proxy core), `requests`, `urllib`, `http.server`, or `asyncio` is **strictly prohibited**.


* **Primitive Sockets:** All network transport mechanisms must use Python's low-level `socket` module explicitly.
* **Single-Threaded I/O Multiplexing:** Concurrency must be achieved through a single-threaded event loop utilizing the `selectors` module (`selectors.DefaultSelector`), leveraging underlying OS primitives like Linux `epoll` or macOS `kqueue`.
* **Memory-Only State:** No disk-bound persistence engines (SQLite/PostgreSQL) are permitted for proxy runtime states. All configuration tables, pooling mechanics, and metric tracking counters must live in-memory.

---

## 3. In-Memory Data Structures & Schema Blueprint

To avoid architectural ambiguity, the AI agent must instantiate and maintain the exact data structures defined below:

### A. Backend Server Pool Registry

Tracks the definitive health state and operational parameters of upstream targets:

```python
backend_pool = {
    "servers": [
        {"id": "srv_1", "host": "127.0.0.1", "port": 8001, "is_healthy": True, "fail_count": 0},
        {"id": "srv_2", "host": "127.0.0.1", "port": 8002, "is_healthy": True, "fail_count": 0},
        {"id": "srv_3", "host": "127.0.0.1", "port": 8003, "is_healthy": True, "fail_count": 0}
    ],
    "current_index": 0,  # For Round-Robin tracking
    "lock": threading.Lock()  # Safeguards updates from the background health thread
}

```

### B. TCP Connection Pool Map

Manages active, pre-warmed connections to bypass handshakes:

```python
# Keys are backend server IDs ("srv_1"). Values are lists of open socket objects.
connection_pool = {
    "srv_1": [],
    "srv_2": [],
    "srv_3": []
}

```

### C. Rate-Limiting Register ("Bouncer Mode")

Tracks client token allocation buckets mapped directly to unique IP strings:

```python
rate_limiter_registry = {
    "client_ip_string": {
        "tokens": 30.0,            # Floating-point token capacity
        "last_updated": 1717592400 # Epoch timestamp (seconds)
    }
}
# Configuration parameters: BUCKET_MAX = 30.0, REFILL_RATE = 2.0 (tokens per second)

```

### D. Active Session Buffer Matrix

Because non-blocking sockets read fragments, the agent must maintain a buffer for partial client transmissions:

```python
# Sockets registered with the event loop map to an active session context
session_buffers = {
    "client_socket_fd": {
        "raw_bytes": b"", 
        "headers_parsed": False,
        "content_length": 0,
        "target_backend_socket": None
    }
}

```

---

## 4. Component Deep Engineering Specifications

### A. Non-Blocking Event-Loop Architecture

* **Initialization:** Bind the master proxy socket to `0.0.0.0:5000`. Invoke `server_socket.setblocking(False)`.
* **Loop Registration:** Register the socket with `selectors.DefaultSelector` for `selectors.EVENT_READ`.
* **The Select Loop:** Implement a continuous operational loop:
```python
while True:
    events = selector.select(timeout=1)
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)

```



### B. Raw HTTP Byte-Parsing Logic & Fragment Assembly

* **The Fragment Problem:** Sockets read up to 4096 bytes. The proxy must append incoming bytes to `session_buffers[fd]["raw_bytes"]`.
* **Delimiter Detection:** The engine must continuously search for the sequence `b"\r\n\r\n"`.
* **Header Splitting:** Once found, split the buffer. Convert the header portion to an ASCII/UTF-8 string. Extract:
* The request line (e.g., `GET /index.html HTTP/1.1`).
* The `Connection:` header state (track whether it says `keep-alive`).
* The `Content-Length:` value for incoming payload boundary validation (`POST` bodies).



### C. Upstream Connection Pooling Lifecycle

```
[Client Request] ──► [Janus Engine] ──► [Checks connection_pool Map]
                                                 │
                    ┌────────────────────────────┴────────────────────────────┐
                    ▼                                                         ▼
        [Available Open Socket found]                            [No Sockets Available]
                    │                                                         │
                    ▼                                                         ▼
    [Reuse Socket: Skip Handshake]                              [Instantiate New Raw TCP Socket]

```

* **Allocation:** When a request is parsed, find the next healthy server via the round-robin counter. Check `connection_pool[server_id]`.
* **Evaluation:** If an idle socket exists, pop it and reuse it immediately. If empty, initialize a fresh raw TCP socket to that target server.
* **Deallocation:** When the upstream server completes its response transmission, instead of closing the backend socket, strip any backend `Connection: close` rules and append that socket object back into `connection_pool[server_id]`.

### D. Multi-Threaded Isolated Health Tracker

* **Concurrently Safe Execution:** Run the tracking sequence inside a dedicated `threading.Thread` loop executing indefinitely every 3 seconds.
* **Socket Verification:** Attempt a non-blocking connection to each server port (`8001`, `8002`, `8003`). If a connection fails or times out within 1.0 second, increment `fail_count`.
* **Threshold Triggering:** If `fail_count >= 3`, acquire `backend_pool["lock"]` and toggle `is_healthy = False`. If an offline server successfully answers 2 consecutive pings, reset `fail_count = 0` and toggle `is_healthy = True`.

---

## 5. System Robustness & Edge-Case Rules

The AI agent must explicitly code logic to handle these exact failure environments:

* **Malformed HTTP Ingress:** If a client transmits random garbage bytes that do not contain a valid HTTP structure or fail to resolve `\r\n\r\n` within a total buffer cap of 8192 bytes, transmit a raw `HTTP/1.1 400 Bad Request\r\n\r\n` byte package and close the socket.
* **Upstream Target Disconnects:** If a reused socket from the `connection_pool` throws a `BrokenPipeError` or returns 0 bytes upon transmission, catch the exception, discard that specific socket, instantiate a new connection from scratch, and retry the request once before returning a `502 Bad Gateway`.

---

## 6. Telemetry Output Matrix (Admin Bridge)

* **JSON API Ingress:** Expose port `5001` using a basic raw socket server loop configured to only serve `GET /metrics`.
* **Output Payload Signature:** Return a strict JSON response containing:
```json
{
  "active_backends": {"srv_1": true, "srv_2": true, "srv_3": false},
  "rate_limited_ips_count": 0,
  "total_processed_requests": 1420,
  "pool_utilization": {"srv_1": 2, "srv_2": 1, "srv_3": 0}
}

```
