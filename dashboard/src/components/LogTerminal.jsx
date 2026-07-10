// components/LogTerminal.jsx
// Scrolling live event log with colour-coded line types

import { useRef, useEffect } from 'react'

function lineClass(type) {
  if (type === 'route')  return 'log-line log-line--route'
  if (type === 'block')  return 'log-line log-line--block'
  if (type === 'warn')   return 'log-line log-line--warn'
  if (type === 'health') return 'log-line log-line--health'
  if (type === 'pool')   return 'log-line log-line--pool'
  return 'log-line'
}

export default function LogTerminal({ logs }) {
  const bottomRef = useRef(null)

  // Auto-scroll is handled by flex-direction: column-reverse in CSS
  // so newest entries naturally appear at top — no JS needed.

  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4, flexShrink: 0 }}>
        <span className="field-label">LIVE EVENT STREAM</span>
        <span style={{ fontSize: 10, color: 'var(--txt-muted)' }}>{logs.length} entries</span>
      </div>
      <div className="log-terminal">
        {logs.map(({ id, ts, line, type }) => (
          <div key={id} className={lineClass(type)}>
            <span style={{ color: 'var(--txt-muted)', marginRight: 6 }}>[{ts}]</span>
            {line}
          </div>
        ))}
        {logs.length === 0 && (
          <div className="log-line" style={{ color: 'var(--txt-muted)', fontStyle: 'italic' }}>
            — awaiting events —
            <span className="blink">_</span>
          </div>
        )}
      </div>
    </div>
  )
}
