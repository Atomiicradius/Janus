// components/Topography.jsx
// Center panel — ASCII/SVG network topology diagram showing
// Janus distributing traffic to three backend nodes

const NODE_W = 90
const NODE_H = 42

function Box({ x, y, label, sublabel, color, glow }) {
  return (
    <g>
      <rect
        x={x} y={y} width={NODE_W} height={NODE_H}
        fill="var(--bg-inset)"
        stroke={color}
        strokeWidth="1.5"
        style={{ filter: `drop-shadow(0 0 6px ${glow})` }}
      />
      <text x={x + NODE_W / 2} y={y + 16} textAnchor="middle"
        fill={color} fontSize="11" fontFamily="var(--font-mono)" fontWeight="700" letterSpacing="1">
        {label}
      </text>
      <text x={x + NODE_W / 2} y={y + 30} textAnchor="middle"
        fill="var(--txt-dim)" fontSize="10" fontFamily="var(--font-mono)">
        {sublabel}
      </text>
    </g>
  )
}

function ServerNode({ x, y, id, port, healthy }) {
  const color = healthy ? 'var(--green-mid)' : 'var(--pink-mid)'
  const glow  = healthy ? 'var(--green-mid)' : 'var(--pink-mid)'
  return (
    <g>
      <rect x={x} y={y} width={NODE_W} height={NODE_H}
        fill={healthy ? 'var(--green-dim)' : 'var(--pink-dim)'}
        stroke={color} strokeWidth="1"
        style={{ filter: `drop-shadow(0 0 4px ${glow})` }}
      />
      <text x={x + NODE_W / 2} y={y + 15} textAnchor="middle"
        fill={color} fontSize="11" fontFamily="var(--font-mono)" fontWeight="700" letterSpacing="1">
        {id.toUpperCase()}
      </text>
      <text x={x + NODE_W / 2} y={y + 28} textAnchor="middle"
        fill="var(--txt-dim)" fontSize="10" fontFamily="var(--font-mono)">
        :{port}
      </text>
      <circle cx={x + NODE_W - 8} cy={y + 8} r={4}
        fill={healthy ? 'var(--green-bright)' : 'var(--pink-bright)'}
        style={{ filter: `drop-shadow(0 0 3px ${color})` }}
      />
    </g>
  )
}

export default function Topography({ data }) {
  const W = 420
  const H = 280

  // Janus box centered at top
  const jX = (W - NODE_W) / 2
  const jY = 20

  // Three server nodes at bottom
  const srvY = H - NODE_H - 20
  const positions = [
    { x: 20,             id: 'srv_01', port: 8001 },
    { x: (W - NODE_W)/2, id: 'srv_02', port: 8002 },
    { x: W - NODE_W - 20, id: 'srv_03', port: 8003 },
  ]

  const backends = data?.backends ?? {}

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
      <svg width={W} height={H} style={{ overflow: 'visible' }}>
        <defs>
          <filter id="glow-green">
            <feGaussianBlur stdDeviation="3" result="coloredBlur" />
            <feMerge><feMergeNode in="coloredBlur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
        </defs>

        {/* Wire lines from Janus to each backend */}
        {positions.map(({ x, id, port }) => {
          const healthy = backends[id]?.healthy !== false
          const color   = healthy ? 'var(--green-dim)' : 'var(--pink-dim)'
          const stroke  = healthy ? '#00cc66' : '#cc1155'
          const fromX   = jX + NODE_W / 2
          const fromY   = jY + NODE_H
          const toX     = x + NODE_W / 2
          const toY     = srvY

          // Animated dash
          return (
            <line key={id}
              x1={fromX} y1={fromY} x2={toX} y2={toY}
              stroke={stroke} strokeWidth="1.5"
              strokeDasharray={healthy ? '6 4' : '3 6'}
              opacity={healthy ? 0.7 : 0.35}
            >
              {healthy && (
                <animate attributeName="strokeDashoffset"
                  from="0" to="-20" dur="0.8s" repeatCount="indefinite" />
              )}
            </line>
          )
        })}

        {/* Janus proxy node */}
        <Box
          x={jX} y={jY}
          label="JANUS" sublabel="0.0.0.0:5000"
          color="var(--blue-bright)"
          glow="var(--blue-bright)"
        />

        {/* Backend server nodes */}
        {positions.map(({ x, id, port }) => (
          <ServerNode
            key={id}
            x={x} y={srvY}
            id={id} port={port}
            healthy={backends[id]?.healthy !== false}
          />
        ))}

        {/* Client traffic arrow */}
        <g>
          <line x1={jX + NODE_W / 2} y1={0} x2={jX + NODE_W / 2} y2={jY}
            stroke="#4488ff" strokeWidth="1.5" strokeDasharray="5 3" opacity="0.6">
            <animate attributeName="strokeDashoffset" from="-20" to="0" dur="0.6s" repeatCount="indefinite" />
          </line>
          <polygon
            points={`${jX + NODE_W/2 - 5},${jY - 2} ${jX + NODE_W/2 + 5},${jY - 2} ${jX + NODE_W/2},${jY + 6}`}
            fill="#4488ff" opacity="0.7"
          />
          <text x={jX + NODE_W / 2 + 8} y={10}
            fill="var(--blue-bright)" fontSize="10" fontFamily="var(--font-mono)" opacity="0.7">
            CLIENT TRAFFIC
          </text>
        </g>

        {/* Rate limit label */}
        {data && (
          <text x={W / 2} y={H - 4} textAnchor="middle"
            fill="var(--txt-muted)" fontSize="10" fontFamily="var(--font-mono)">
            TRACKED IPs: {data.rate_limits?.tracked_ips ?? 0}
            {'  '}|{'  '}
            POOL SOCKETS: {Object.values(data.pools ?? {}).reduce((a, b) => a + b, 0)}
          </text>
        )}
      </svg>
    </div>
  )
}
