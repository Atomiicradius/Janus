# Janus 🪐

A custom, high-performance **Layer 7 Reverse Proxy and Load Balancer** engineered from low-level network primitives. Built entirely in Python using raw socket manipulation and synchronous I/O multiplexing, Janus acts as an intelligent traffic gatekeeper, distributing network loads across an upstream server farm while exposing a retro-futuristic administrative control console.

---

## The Architectural Constraints (TRD Compliance)
To maximize performance and keep execution close-to-the-metal, Janus operates under strict architectural constraints:
* **Zero Async Frameworks:** Built without `asyncio`, `FastAPI`, `Flask` (for the engine core), or advanced stdlib HTTP modules (`http.server`).
* **I/O Multiplexing:** Leverages a single-threaded execution thread driven by primitive OS kernel polling hooks via the Python `selectors` module.
* **Manual Protocol Analysis:** Parses byte streams directly from hardware buffers in max 4096-byte chunks, monitoring boundary flags (`\r\n\r\n`) to decompose HTTP packets.
* **Pure Volatile State:** All tracking caches, connection pools, and operational telemetry metrics live strictly in RAM-allocated Python data structures.

---

## Key Features

### 1. Layer 7 Stream Parsing & Round-Robin Routing
Inspects incoming client headers at the application layer to validate HTTP compliance, tracking methods, targets, and payloads. Balances client volumes smoothly across healthy downstream target backends using a deterministic round-robin counter.

### 2. High-Performance TCP Connection Pooling
Bypasses the CPU-intensive 3-way TCP handshake latency overhead by caching open, idle, warm downstream socket connections in an in-memory pool mapping, recycling them instantly for subsequent transactions.

### 3. "Bouncer Mode" Token-Bucket Rate Limiter
Guards against volumetric overloads using a low-overhead tracking cache. Computes mathematical token replenishment on-demand per unique client IP address, instantly injecting raw `HTTP/1.1 429 Too Many Requests` byte blocks to terminate flood vectors.

### 4. Background Active Health Monitor
Spins up an isolated health-checking daemon thread that periodically scans targeted backend ports using swift connection timeouts, dynamically restructuring the available live server pool without causing data race conditions.

---

## Dashboard Aesthetic: Retro-Systems Utility
The administration interface embraces a dense, high-contrast visual identity inspired by vintage **Windows 95 system utilities and CRT monitor telemetry displays**. It delivers high informational density across scrollable data tables, real-time node topology networks, and green-phosphor tracking charts.

---

## Project Development Roadmap
- [x] **Phase 1:** Non-Blocking Socket Foundation & Core Selector Event Loop
- [x] **Phase 2:** Byte-Stream Fragmentation Accumulation & HTTP Request Header Parser
- [ ] **Phase 3:** Downstream Proxy Routing, Round-Robin Balancing, and Isolated Server Farm
- [ ] **Phase 4:** Threaded Health Checking, Mutex Isolation, and TCP Socket Reusing Pool
- [ ] **Phase 5:** In-Memory Token-Bucket Rate-Limiting ("Bouncer Mode")
- [ ] **Phase 6:** Telemetry JSON Serialization APIs & Y2K CRT Frontend Matrix