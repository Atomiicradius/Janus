// App.jsx — Janus Y2K CRT Admin Dashboard

import { useTelemetry } from './useTelemetry'
import WinPanel    from './components/WinPanel'
import StatsBanner from './components/StatsBanner'
import NodeGrid    from './components/NodeGrid'
import Topography  from './components/Topography'
import LogTerminal from './components/LogTerminal'

function Taskbar({ data, error, clock }) {
  const online = !error && data !== null
  const timeStr = clock.toLocaleTimeString('en-GB', { hour12: false })
  const dateStr = clock.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })

  const healthy = data
    ? Object.values(data.backends).filter(s => s.healthy).length
    : 0
  const total = data ? Object.keys(data.backends).length : 3

  return (
    <div className="taskbar">
      <div className="taskbar__left">
        <button className="start-btn">⊞ JANUS</button>
        <div className="sys-chip">
          <div className={`sys-chip__dot sys-chip__dot--${online ? 'live' : 'dead'}`} />
          <span style={{ color: online ? 'var(--green-bright)' : 'var(--pink-bright)' }}>
            {online ? 'API CONNECTED' : 'API OFFLINE'}
          </span>
        </div>
        <div className="sys-chip">
          <span style={{ color: 'var(--txt-dim)' }}>NODES:</span>
          <span style={{ color: healthy === total ? 'var(--green-bright)' : 'var(--amber-warn)', marginLeft: 4 }}>
            {healthy}/{total} HEALTHY
          </span>
        </div>
        {data && (
          <div className="sys-chip">
            <span style={{ color: 'var(--txt-dim)' }}>POOL:</span>
            <span style={{ color: 'var(--blue-bright)', marginLeft: 4 }}>
              {Object.values(data.pools).reduce((a, b) => a + b, 0)} WARM
            </span>
          </div>
        )}
      </div>
      <div className="taskbar__right">
        <span style={{ color: 'var(--txt-dim)' }}>ADMIN PORT 5001</span>
        <div style={{ width: 1, height: 14, background: 'var(--border-hard)' }} />
        <span>{dateStr}</span>
        <span style={{ color: 'var(--green-bright)', fontWeight: 700 }}>{timeStr}</span>
      </div>
    </div>
  )
}

export default function App() {
  const { data, error, logs, clock } = useTelemetry()

  return (
    <div style={{
      width: '100vw', height: '100vh',
      display: 'flex', flexDirection: 'column',
      background: 'var(--bg-void)',
      overflow: 'hidden',
    }}>

      {/* ── Top strip: system title bar ── */}
      <div style={{
        background: 'var(--blue-title)',
        borderBottom: '2px solid #2255aa',
        padding: '5px 12px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexShrink: 0,
      }}>
        <span style={{ fontSize: 13, fontWeight: 700, color: '#fff', letterSpacing: '0.14em', textTransform: 'uppercase' }}>
          ▣ JANUS LOAD BALANCER — ADMINISTRATIVE CONTROL PANEL v1.0
        </span>
        <span style={{ fontSize: 11, color: '#8899cc', letterSpacing: '0.08em' }}>
          {error
            ? <span style={{ color: 'var(--pink-bright)' }}>⚠ NO SIGNAL — {error}</span>
            : <span style={{ color: 'var(--green-mid)' }}>◉ LIVE — polling every 1000ms</span>
          }
        </span>
      </div>

      {/* ── Stats banner ── */}
      <div style={{ padding: '8px 10px 0', flexShrink: 0 }}>
        <StatsBanner data={data} />
      </div>

      {/* ── Main three-column layout ── */}
      <div style={{
        flex: 1,
        display: 'grid',
        gridTemplateColumns: '1fr 1.1fr 1fr',
        gap: 8,
        padding: '8px 10px',
        minHeight: 0,
      }}>

        {/* ── LEFT: Packet Ledger (log terminal) ── */}
        <WinPanel title="PACKET LEDGER" icon="📋" style={{ minHeight: 0 }}>
          <LogTerminal logs={logs} />
        </WinPanel>

        {/* ── CENTER: Topography Console + Node Grid ── */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8, minHeight: 0 }}>
          <WinPanel title="NETWORK TOPOGRAPHY" icon="🔗" style={{ flex: '0 0 auto' }}>
            <div style={{ height: 280 }}>
              <Topography data={data} />
            </div>
          </WinPanel>
          <WinPanel title="BACKEND NODE REGISTRY" icon="⬡" style={{ flex: 1, minHeight: 0 }}>
            <NodeGrid data={data} />
          </WinPanel>
        </div>

        {/* ── RIGHT: Analytics + Rate limiter ── */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8, minHeight: 0 }}>
          <WinPanel title="RATE LIMITER STATUS" icon="🛡" style={{ flex: '0 0 auto' }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
                <div className="stat-chip stat-chip--pink">
                  <span className="stat-chip__label">TOTAL BLOCKED</span>
                  <span className="stat-chip__value" style={{ fontSize: 20 }}>
                    {data?.metrics?.total_blocked_connections ?? '—'}
                  </span>
                </div>
                <div className="stat-chip stat-chip--amber">
                  <span className="stat-chip__label">TRACKED IPs</span>
                  <span className="stat-chip__value" style={{ fontSize: 20 }}>
                    {data?.rate_limits?.tracked_ips ?? '—'}
                  </span>
                </div>
              </div>
              <div style={{ background: 'var(--bg-inset)', border: '1px solid var(--border-hard)', padding: '8px 10px', fontSize: 11 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span style={{ color: 'var(--txt-dim)' }}>BUCKET CAPACITY</span>
                  <span style={{ color: 'var(--green-mid)' }}>30.0 tok</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span style={{ color: 'var(--txt-dim)' }}>REFILL RATE</span>
                  <span style={{ color: 'var(--green-mid)' }}>2.0 tok/s</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: 'var(--txt-dim)' }}>ALGORITHM</span>
                  <span style={{ color: 'var(--blue-bright)' }}>TOKEN BUCKET</span>
                </div>
              </div>
            </div>
          </WinPanel>

          <WinPanel title="ANALYTICS CLUSTER" icon="📊" style={{ flex: 1, minHeight: 0 }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8, height: '100%' }}>
              {/* Pool utilisation bars per backend */}
              <span className="field-label">POOL UTILISATION / BACKEND</span>
              {(data
                ? Object.entries(data.pools)
                : [['srv_01', 0], ['srv_02', 0], ['srv_03', 0]]
              ).map(([id, depth]) => {
                const pct = Math.min(100, (depth / 8) * 100)
                const healthy = data?.backends?.[id]?.healthy !== false
                return (
                  <div key={id} style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11 }}>
                      <span style={{ color: healthy ? 'var(--green-mid)' : 'var(--pink-mid)' }}>{id.toUpperCase()}</span>
                      <span style={{ color: 'var(--txt-dim)' }}>{depth}/8</span>
                    </div>
                    <div className="pool-bar-track">
                      <div
                        className={`pool-bar-fill${!healthy ? ' pool-bar-fill--dead' : ''}`}
                        style={{ width: `${pct}%`, transition: 'width 0.4s ease' }}
                      />
                    </div>
                  </div>
                )
              })}

              <div className="h-divider" style={{ marginTop: 8 }} />

              {/* Throughput indicator */}
              <span className="field-label">THROUGHPUT TREND</span>
              <div style={{ background: 'var(--bg-inset)', border: '1px solid var(--border-hard)', padding: '8px 10px', fontSize: 11 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span style={{ color: 'var(--txt-dim)' }}>TOTAL BYTES</span>
                  <span style={{ color: 'var(--green-bright)' }}>
                    {data
                      ? (() => {
                          const b = data.metrics.aggregate_bytes_transferred
                          return b < 1024 ? `${b} B`
                            : b < 1048576 ? `${(b/1024).toFixed(1)} KB`
                            : `${(b/1048576).toFixed(2)} MB`
                        })()
                      : '—'
                    }
                  </span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span style={{ color: 'var(--txt-dim)' }}>TOTAL REQUESTS</span>
                  <span style={{ color: 'var(--green-bright)' }}>
                    {data?.metrics?.total_requests_processed ?? '—'}
                  </span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: 'var(--txt-dim)' }}>UPTIME</span>
                  <span style={{ color: 'var(--amber-warn)' }}>
                    {data ? `${data.uptime_seconds.toFixed(0)}s` : '—'}
                  </span>
                </div>
              </div>

              {/* Health sweep indicator */}
              <div className="h-divider" />
              <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                background: 'var(--bg-inset)', border: '1px solid var(--border-hard)',
                padding: '6px 10px', fontSize: 11,
              }}>
                <span style={{ color: 'var(--txt-dim)' }}>HEALTH MONITOR</span>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span className="status-dot status-dot--up pulse-green" />
                  <span style={{ color: 'var(--green-mid)' }}>ACTIVE — 3s interval</span>
                </div>
              </div>
            </div>
          </WinPanel>
        </div>
      </div>

      {/* ── Bottom taskbar ── */}
      <Taskbar data={data} error={error} clock={clock} />
    </div>
  )
}
