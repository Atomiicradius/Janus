// components/NodeGrid.jsx
// Backend server status tiles with pool depth bars

const POOL_MAX = 8

function PoolBar({ depth, healthy }) {
  const pct = Math.min(100, (depth / POOL_MAX) * 100)
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
        <span className="field-label">WARM POOL</span>
        <span style={{ fontSize: 11, color: 'var(--txt-label)' }}>{depth}/{POOL_MAX}</span>
      </div>
      <div className="pool-bar-track">
        <div
          className={`pool-bar-fill${!healthy ? ' pool-bar-fill--dead' : ''}`}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  )
}

function NodeTile({ id, srv, pool }) {
  const up = srv.healthy
  return (
    <div className={`node-tile node-tile--${up ? 'up' : 'down'} ${up ? 'pulse-green' : 'pulse-pink'}`}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span className="node-tile__id" style={{ color: up ? 'var(--green-bright)' : 'var(--pink-bright)' }}>
          {id.toUpperCase()}
        </span>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span className={`status-dot status-dot--${up ? 'up' : 'down'} ${up ? 'pulse-green' : 'pulse-pink'}`} />
          <span style={{ fontSize: 10, color: up ? 'var(--green-mid)' : 'var(--pink-mid)', letterSpacing: '0.1em' }}>
            {up ? 'ONLINE' : 'OFFLINE'}
          </span>
        </div>
      </div>

      <div className="h-divider" />

      <div className="node-tile__row">
        <span className="node-tile__key">HOST</span>
        <span className="node-tile__val">{srv.host}</span>
      </div>
      <div className="node-tile__row">
        <span className="node-tile__key">PORT</span>
        <span className="node-tile__val">{srv.port}</span>
      </div>
      <div className="node-tile__row">
        <span className="node-tile__key">FAILURES</span>
        <span className="node-tile__val" style={{ color: srv.consecutive_failures > 0 ? 'var(--amber-warn)' : 'var(--txt-primary)' }}>
          {srv.consecutive_failures}
        </span>
      </div>

      <div className="h-divider" />
      <PoolBar depth={pool ?? 0} healthy={up} />
    </div>
  )
}

export default function NodeGrid({ data }) {
  if (!data) return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 8, height: '100%', alignContent: 'start' }}>
      {['srv_01','srv_02','srv_03'].map(id => (
        <div key={id} className="node-tile" style={{ opacity: 0.4 }}>
          <span className="node-tile__id">{id.toUpperCase()}</span>
          <span style={{ fontSize: 11, color: 'var(--txt-muted)' }}>AWAITING DATA…</span>
        </div>
      ))}
    </div>
  )

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 8 }}>
      {Object.entries(data.backends).map(([id, srv]) => (
        <NodeTile key={id} id={id} srv={srv} pool={data.pools[id] ?? 0} />
      ))}
    </div>
  )
}
