File 1: Product Requirements Document (PRD)
Project Name: Janus
Project Title: Custom Layer 7 Load Balancer with Byte-Stream Parsing & TCP Connection Pooling
1. Project Overview
Janus is a high-performance, user-space Layer 7 reverse proxy and load balancer built completely from scratch. Acting as a gatekeeper sitting between public client traffic and a private backend server fleet, Janus handles raw byte streams, parses incoming HTTP/1.1 requests, and intelligently distributes traffic to optimize availability, throughput, and system security. It includes an active health-monitoring daemon, an advanced application-layer rate limiter ("Bouncer Mode"), and an interactive telemetry dashboard for real-time monitoring and fault visualization.

2. Core Functional Requirements
Component 1: Reverse Proxy & Load Balancing Engine
Single-Point Ingress: Accept incoming TCP connections on a designated public-facing port (e.g., 5000).

Layer 7 Traffic Distribution: Cycle client requests across a pool of available backend web servers using an active Round-Robin routing strategy.

Manual Byte Manipulation: Read and parse raw data streams directly from socket buffers to interpret the HTTP protocol format without third-party web frameworks.

Response Forwarding: Capture backend HTTP server responses in their entirety and relay them transparently back to the original client browser connection.

Component 2: Active Health Monitor (Heartbeat Daemon)
Background Heartbeats: Execute periodic asynchronous pings (every 3–5 seconds) to evaluate the status of all registered backend servers.

Dynamic Failover Execution: Automatically isolate any target server failing to respond to sequential health checks. Remove it instantly from the live routing table.

Auto-Recovery Restoration: Automatically restore a recovered backend to the active routing rotation as soon as it successfully passes sequential health check thresholds.

Component 3: "Bouncer Mode" Application-Layer Security
Targeted Rate Limiting: Track incoming client traffic patterns based strictly on unique client IP addresses.

Token-Bucket Architecture: Implement a low-overhead tracking algorithm to allocate a set burst token pool per client IP.

Proactive Traffic Throttling: Instantly drop incoming packets and return an HTTP 429 Too Many Requests status block if a client attempts to flood the system beyond configured request thresholds.

Component 4: Real-Time Telemetry Admin Dashboard
Dynamic Metric Tracking: Deliver live visual telemetry reflecting total concurrent connections, data throughput volumes (bytes/sec), and average connection processing latencies.

Fleet Status Mapping: Visually render the structural network layout showing the operational health states (Online/Offline) of all target servers.

Simulated Fault Injection: Feature physical dashboard control toggles allowing administrators to manually simulate server blackouts or trigger artificial traffic spikes to test real-time failover operations.

3. System Scope & Boundaries
In Scope
Handling standard HTTP/1.1 GET, POST, and OPTIONS request frameworks.

Parsing mandatory HTTP connection properties (Host, Connection: keep-alive, Content-Length).

In-memory data management for real-time routing metrics, active rate limiting arrays, and backend server configurations.

Single-thread asynchronous I/O loops handling thousands of concurrent connections.

Out of Scope (For Phase 1)
HTTPS/TLS/SSL termination, certificate management, or encryption handshakes.

Persistent disk-space databases (all operational states must stay completely in-memory for minimal overhead).

Static asset edge-caching or caching headers processing.

4. Success Criteria
Zero Drop Failover: A deliberate failure of a backend server must seamlessly trigger routing changes without throwing 502 Bad Gateway errors to incoming client requests.

High Concurrency Processing: The engine must handle thousands of persistent client connections without bottlenecking or consuming excessive OS resources.

Microsecond Overhead: The inner proxy loop must minimize internal latency additions, introducing nominal transport lag to raw backend server times.