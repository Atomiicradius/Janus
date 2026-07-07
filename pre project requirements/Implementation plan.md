# File 6: Step-by-Step Implementation Plan

## Project Name: Janus

## Execution Framework: Google Antigravity + Scoped Awesome-Skills

---

## Phase 1: The Non-Blocking Socket Foundation

* **Target Skills to Load:** `@backend`, `@infrastructure`
* **Objective:** Establish a rock-solid, single-threaded asynchronous TCP listener using primitive socket mechanics.
* **Step 1.1:** Initialize a raw master socket bound to `0.0.0.0:5000` and configure it to non-blocking mode via `server_socket.setblocking(False)`.
* **Step 1.2:** Integrate the `selectors` module, registering the master socket for `selectors.EVENT_READ` to capture incoming connection intents.
* **Step 1.3:** Implement the core `while True:` execution select loop, routing events to an `accept_handler` callback that non-blockingly tracks new client file descriptors (`fd`).
* **Verification Boundary:** Run the script and use a terminal utility like `netcat` or `curl` to connect, confirming the proxy logs connection attachments without stalling or spawning threads.

## Phase 2: Byte-Stream Accumulation & HTTP Request Parsing

* **Target Skills to Load:** `@backend`, `@infrastructure`
* **Objective:** Read fragmented data packages directly from the operating system network buffers and reconstruct the application-layer headers.
* **Step 2.1:** Construct the in-memory `session_buffers` dictionary framework to isolate storage fragments per active socket file descriptor.
* **Step 2.2:** Program a read loop reading max 4096-byte segments (`socket.recv(4096)`) from readable client descriptors, gracefully managing `BlockingIOError` states.
* **Step 2.3:** Implement manual byte parsing that continuously scans for the boundary delimiter marker `b"\r\n\r\n"`. Once found, split the byte string and extract the clear-text HTTP Request headers.
* **Verification Boundary:** Use a web browser to visit `http://localhost:5000`. The engine must print out the precisely isolated request headers (e.g., `Method`, `Path`, `User-Agent`) and gracefully close the client loop.

## Phase 3: Downstream Proxy Routing & Round-Robin Balance

* **Target Skills to Load:** `@backend`, `@infrastructure`
* **Objective:** Connect Janus to external, isolated backend target servers.
* **Step 3.1:** Construct 3 independent, minimalist dummy web applications running on isolated local host ports (`8001`, `8002`, `8003`) using Flask to mimic a server farm.


* **Step 3.2:** Write the distribution counter mechanism in Janus that cycles through healthy backends sequentially using standard Round-Robin pointer logic.
* **Step 3.3:** When a request is fully assembled, open a downstream non-blocking socket to the chosen backend server, transmit the original client byte payload, await the return byte packet stream, and relay those exact bytes back to the originating client.
* **Verification Boundary:** Repeatedly refreshing your browser on port 5000 must cleanly cycle through outputs from Server 1, Server 2, and Server 3 in a balanced order.

## Phase 4: Threaded Health Monitoring & TCP Connection Pooling

* **Target Skills to Load:** `@backend`, `@infrastructure`, `@security`
* **Objective:** Optimize internal latency and introduce topology protection.
* **Step 4.1:** Spin up a dedicated background `threading.Thread` loop that executes every 3 seconds to ping backend ports natively via fast TCP socket timeouts.
* **Step 4.2:** Implement thread-safe mutex locking (`threading.Lock`) around the central `backend_pool` dictionary to prevent state updates from causing data race conditions in the main event loop.
* **Step 4.3:** Build the `socket_pool` map registry. Modify the routing engine: instead of creating a fresh socket for every user action, it must attempt to pull an already-warm, persistent open socket out of the idle pool list.
* **Verification Boundary:** Intentionally shut down Server 2. Janus must automatically detect the change, modify its layout map seamlessly, and route subsequent client clicks exclusively to Server 1 and 3 without dropping packets.

## Phase 5: The "Bouncer" Rate Limiter

* **Target Skills to Load:** `@security`, `@backend`
* **Objective:** Guard the application layer from external volumetric overloads.
* **Step 5.1:** Implement the mathematical Token-Bucket calculations inside the client processing lifecycle.
* **Step 5.2:** Program an intercept function: if a client's tracked IP address exhausts its token pool balance, completely abort proxy routing and immediately inject a raw `HTTP/1.1 429 Too Many Requests` byte block back out the socket interface.
* **Verification Boundary:** Set a temporary strict rule limit of 2 requests per user. Spam your browser refresh button; the interface must instantly snap open a raw 429 restriction block.

## Phase 6: Telemetry Endpoint Integration & Retro UI Construction

* **Target Skills to Load:** `@frontend`
* **Objective:** Provide administrative visibility using our custom retro layout framework.
* **Step 6.1:** Open a secondary administrative port loop (`5001`) that intercepts basic `GET /api/v1/telemetry` requests to output serialized runtime statistics in a structured JSON string format.
* **Step 6.2:** Set up a React frontend application initialized with standard CSS variables matching the classic Win95/CRT layout specs shown in your generated mockup.


* **Step 6.3:** Wire up background interval fetch states connecting to port 5001 to pipe real-time numeric data arrays straight into your dashboard components and terminal grids.