// components/WinPanel.jsx
// Reusable Win95-style window panel with title bar

export default function WinPanel({ title, icon = '▣', children, style }) {
  return (
    <div className="panel" style={style}>
      <div className="win-titlebar">
        <span className="win-titlebar__title">{icon} {title}</span>
        <div className="win-titlebar__controls">
          <div className="win-btn">_</div>
          <div className="win-btn">▢</div>
          <div className="win-btn">✕</div>
        </div>
      </div>
      <div className="panel__body" style={{ overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        {children}
      </div>
    </div>
  )
}
