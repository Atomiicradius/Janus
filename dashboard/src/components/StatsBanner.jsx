// components/StatsBanner.jsx
// Top-of-dashboard stat chips: requests, blocked, bytes, uptime

import { fmt, fmtBytes } from '../useTelemetry'

function StatChip({ label, value, variant }) {
  return (
    <div className={`stat-chip stat-chip--${variant}`}>
      <span className="stat-chip__label">{label}</span>
      <span className="stat-chip__value">{value}</span>
    </div>
  )
}

function fmtUptime(sec) {
  const h = Math.floor(sec / 3600)
  const m = Math.floor((sec % 3600) / 60)
  const s = Math.floor(sec % 60)
  return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`
}

export default function StatsBanner({ data }) {
  if (!data) return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 6 }}>
      {['REQUESTS', 'BLOCKED', 'BYTES TX', 'UPTIME'].map(l => (
        <div className="stat-chip" key={l}>
          <span className="stat-chip__label">{l}</span>
          <span className="stat-chip__value" style={{ color: 'var(--txt-muted)', fontSize: 18 }}>—</span>
        </div>
      ))}
    </div>
  )

  const { metrics, uptime_seconds } = data
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 6 }}>
      <StatChip label="REQUESTS PROCESSED"   value={fmt(metrics.total_requests_processed)}   variant="green" />
      <StatChip label="CONNECTIONS BLOCKED"  value={fmt(metrics.total_blocked_connections)}  variant="pink"  />
      <StatChip label="BYTES RELAYED"        value={fmtBytes(metrics.aggregate_bytes_transferred)} variant="blue"  />
      <StatChip label="UPTIME"               value={fmtUptime(uptime_seconds)}                variant="amber" />
    </div>
  )
}
