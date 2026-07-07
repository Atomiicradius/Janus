File 5: In-Memory State & Telemetry Data Schema
Project Name: Janus
Project Title: Custom Layer 7 Load Balancer with Byte-Stream Parsing & TCP Connection Pooling
1. Architectural Philosophy
To achieve high-concurrency packet routing, Janus does not utilize a traditional persistent relational or NoSQL database. All system states, session memory fragments, connection maps, and security data registers live strictly in volatile, RAM-allocated Python data structures. Data mutations must be isolated, low-overhead, and thread-safe.

2. In-Memory Python Data Structures (Engine Core)
The AI agent must initialize and maintain the following four data structures within the main Python daemon process:

A. The Ingress Session Buffer (session_buffers)
Type: Dict[int, Dict[str, Any]] (Key: Sockets File Descriptor fd integer)

Purpose: Tracks fragmented, non-blocking byte components received from client connections before they are complete enough to forward upstream.

Schema Specification:

Python
session_buffers = {
    42: {  # Example client socket file descriptor
        "client_ip": "192.168.1.101",
        "raw_payload": b"GET /analytics HTTP/1.1\r\nHost: localhost:5000\r\n\r\n",
        "headers_completed": True,
        "bytes_expected": 0,       # Non-zero only for POST requests with Content-Length
        "backend_socket_fd": 55    # Maps to the corresponding socket linked upstream
    }
}
B. The Live Server Cluster Registry (backend_pool)
Type: Dict[str, Any]

Purpose: The single source of truth for load distribution and backend network status.

Schema Specification:

Python
backend_pool = {
    "active_index": 0,  # Pointer tracking the current target for Round-Robin distribution
    "servers": {
        "srv_01": {"host": "127.0.0.1", "port": 8001, "healthy": True, "consecutive_failures": 0},
        "srv_02": {"host": "127.0.0.1", "port": 8002, "healthy": True, "consecutive_failures": 0},
        "srv_03": {"host": "127.0.0.1", "port": 8003, "healthy": True, "consecutive_failures": 0}
    }
}
C. The Persistent Connection Pool (socket_pool)
Type: Dict[str, List[socket.socket]] (Key: Server ID string)

Purpose: Stores open, idle, warm TCP sockets connected to the backends to eliminate connection setup handshakes.

Schema Specification:

Python
socket_pool = {
    "srv_01": [<socket.socket fd=12, family=AddressFamily.AF_INET, type=SocketKind.SOCK_STREAM>],
    "srv_02": [], # Empty indicates no idle sockets; a new connection handshake must fire
    "srv_03": []
}
D. The Rate-Limiting Sliding Token Cache (rate_limit_cache)
Type: Dict[str, Dict[str, Any]] (Key: Client string IP address)

Purpose: Low-overhead ledger for executing the "Bouncer Mode" token-bucket algorithm.

Schema Specification:

Python
rate_limit_cache = {
    "192.168.1.101": {
        "tokens": 24.5,            # Remaining token allocation balance
        "last_replenished": 1717616105.122  # High-resolution UNIX float epoch timestamp
    }
}
3. Administrative Bridge API Specification (Port 5001)
To populate your visual dashboards dynamically, the backend must serialize its in-memory states and expose them via a lightweight administrative port.

Endpoint: GET /api/v1/telemetry
Format: application/json

Target Response Body Schema:

JSON
{
  "system_status": "ACTIVE",
  "uptime_seconds": 1260,
  "global_metrics": {
    "total_requests_processed": 84920,
    "aggregate_bytes_transferred": 419430400,
    "average_proxy_latency_ms": 1.24
  },
  "rate_limiting": {
    "active_tracked_ips": 14,
    "total_blocked_connections": 142
  },
  "cluster_topology": [
    {
      "id": "srv_01",
      "endpoint": "127.0.0.1:8001",
      "status": "HEALTHY",
      "active_pooled_connections": 4
    },
    {
      "id": "srv_02",
      "endpoint": "127.0.0.1:8002",
      "status": "HEALTHY",
      "active_pooled_connections": 2
    },
    {
      "id": "srv_03",
      "endpoint": "127.0.0.1:8003",
      "status": "FAULT_INJECTED",
      "active_pooled_connections": 0
    }
  ]
}
Endpoint: POST /api/v1/chaos
Format: application/json

Request Body Schema:

JSON
{
  "target_server_id": "srv_03",
  "action": "TOGGLE_FAULT",
  "value": true
}
System Action: Intercepts the operational loop, updates backend_pool["servers"]["srv_03"]["healthy"] = False, and drops all active sockets pooled inside socket_pool["srv_03"].