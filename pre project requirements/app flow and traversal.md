
# File 3: App Flow & Packet Traversal Matrix

## Project Name: Janus

## Project Title: Custom Layer 7 Load Balancer with Byte-Stream Parsing & TCP Connection Pooling

---

## 1. Interaction Flow A: The Administrator Telemetry Dashboard

This section dictates how an engineer or administrator interacts with the **Janus** web interface to monitor traffic and test system robustness.

### Step 1: Initial Ingress & Handshake

* **Action:** The administrator opens a web browser and navigates to the React/Vite development server (default: `http://localhost:3000`).
* **System Response:** The React application loads its layout framework and instantly fires a background fetch request to the Janus Admin Metrics API running on port `5001` (`GET http://localhost:5001/metrics`).

### Step 2: Live Telemetry Synchronization

* **Action:** The user remains on the index view.
* **System Response:** A React `useEffect` hook instantiates a `setInterval` routine that polls the metrics endpoint every 1000 milliseconds (1 second). The system feeds live data arrays directly into Chart.js elements, causing the visual request-per-second graph and server latency trackers to tick smoothly across the screen.

### Step 3: Triggering Simulated Failures (Chaos Testing)

* 
**Action:** The administrator clicks a red toggle button next to "Mock Backend 3 (Port 8003)" labeled **[Inject Fault]**.


* **System Response:** 1. The frontend dispatches an administrative payload via a HTTP `POST` to the admin API on port `5001`.
2. The Janus metrics engine captures this request and instantly forces `backend_pool["servers"][2]["is_healthy"] = False` without waiting for the background heartbeat thread to detect it.
3. On the dashboard interface, the green terminal circle next to Server 3 instantly changes to a flashing red cross banner, and its tracked connection metrics drop immediately to zero.

---

## 2. Interaction Flow B: The Low-Level Packet Traversal Matrix

This section dictates exactly how a single client HTTP request is physically handled, translated, routed, and replied to inside the single-threaded asynchronous networking loop.

```
 [Public Client]              [Janus Core Engine]             [Upstream Fleet]
  (Browser/Curl)                  (Port 5000)                  (Ports 8001-8003)
        │                             │                               │
        │─── 1. TCP Handshake ───────►│                               │
        │─── 2. Send Raw Bytes ──────►│                               │
        │                             │─── 3. Check Conn Pool ───────►│
        │                             │    (Reuse or New Handshake)   │
        │                             │─── 4. Forward Bytes ─────────►│
        │                             │◄── 5. Receive Response ───────│
        │◄── 6. Relay Response ───────│                               │
        │                             │─── 7. Recycle Upstream Socket │
        X (Close Client Connection)   │                               │

```

### Phase 1: Client Ingress & Connection Acceptance

* **Trigger:** A client (e.g., a web browser or a `curl` terminal command) attempts to open a network connection to `http://localhost:5000`.
* **State Change:** The operating system finishes the TCP 3-way handshake. The master proxy socket flags a read-ready event inside the `selectors` pool.
* **Loop Execution:** The event loop wakes up, grabs the callback function, executes `client_socket, client_address = master_socket.accept()`, instantly configures `client_socket.setblocking(False)`, and registers this new client descriptor to listen for incoming bytes.

### Phase 2: Rate-Limit Evaluation ("The Bouncer")

* **Trigger:** The newly registered client socket sends data.
* **State Change:** The loop detects a readable event on the client file descriptor.
* **Loop Execution:** 1. Extract the client's string IP from the connection object.
2. Query `rate_limiter_registry`. Calculate token replenishment based on time delta since the last timestamp.
3. **Branch 1 (Allowed):** If tokens > 1, decrement the token pool by exactly 1.0, update the timestamp, and proceed to Phase 3.
4. **Branch 2 (Blocked):** If tokens < 1, instantly execute `client_socket.sendall(b"HTTP/1.1 429 Too Many Requests...")`, completely bypass the routing logic, remove the socket from the selector, and close the descriptor.

### Phase 3: Chunk Reading & HTTP Header Extraction

* **Trigger:** The client is allowed through the rate-limiting gate.
* **State Change:** The engine calls `data = client_socket.recv(4096)`.
* **Loop Execution:** 1. Append `data` to `session_buffers[client_fd]["raw_bytes"]`.
2. Scan the byte stream looking for the sequence `b"\r\n\r\n"`.
3. If the delimiter isn't found yet, exit the callback and wait for the loop to flag more incoming bytes from this socket.
4. Once `b"\r\n\r\n"` is identified, flip `session_buffers[client_fd]["headers_parsed"] = True`, read the request line text, and extract metadata parameters.

### Phase 4: Target Selection & Connection Pooling Selection

* **Trigger:** Headers have been successfully validated and parsed.
* **State Change:** The load balancer looks up where to send this payload.
* **Loop Execution:**
1. Query `backend_pool` to find the next available server index where `is_healthy == True` using round-robin logic.
2. Check `connection_pool[selected_server_id]`.
3. **Branch 1 (Pool Match):** If an open socket exists in the list, pop it out. Instantly use it. (Saves time by skipping the connection setup handshake).
4. **Branch 2 (Pool Miss):** If the list is empty, initialize a brand new raw socket `upstream_socket = socket.socket()`, set it to non-blocking, and execute a network connection to that specific backend target port (e.g., `8001`).



### Phase 5: Request Forwarding & Response Relay

* **Trigger:** An upstream backend connection is established and ready.
* **State Change:** Moving data across the bridge.
* **Loop Execution:**
1. Forward the compiled client header byte payload directly down the upstream socket to the backend server.
2. Register the upstream socket with the `selectors` loop to watch for a response.
3. When the backend server responds, read its response payload in 4096-byte blocks.
4. Pipe those exact byte blocks straight back to the original `client_socket`.
5. Track data throughput and calculate processing time latency to update the global metrics tracker.



### Phase 6: Connection Deallocation & Recycling

* **Trigger:** The backend server finishes sending its response (detected by parsing the backend's `Content-Length` header or receiving a zero-byte read indicating termination).
* **State Change:** Cleaning up connection states to stay fast.
* **Loop Execution:**
1. Remove the `client_socket` from the selector ring and close it completely to free up the OS file descriptor.
2. Delete the temporary tracking record inside `session_buffers`.
3. Do **not** close the upstream backend socket. Clean its active state, bypass any termination headers, and push that open socket object directly back into the array pool (`connection_pool[server_id].append(upstream_socket)`) so the next user request can immediately reuse it.

