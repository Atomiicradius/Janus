// useTelemetry.js
// Polls /api/v1/metrics every 1000ms and maintains a live event log.

import { useState, useEffect, useRef, useCallback } from 'react'

const API_URL = 'http://127.0.0.1:5001/api/v1/metrics'
const POLL_MS = 1000
const MAX_LOG = 120

function fmt(n) {
  return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ',')
}

function fmtBytes(b) {
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  return `${(b / (1024 * 1024)).toFixed(2)} MB`
}

export function useTelemetry() {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [logs, setLogs] = useState([])
  const [clock, setClock] = useState(new Date())
  const prevData = useRef(null)
  const pollCount = useRef(0)

  // Wall clock tick
  useEffect(() => {
    const id = setInterval(() => setClock(new Date()), 1000)
    return () => clearInterval(id)
  }, [])

  const pushLog = useCallback((line, type = '') => {
    const ts = new Date().toISOString().slice(11, 23)
    setLogs(prev => {
      const next = [{ ts, line, type, id: Date.now() + Math.random() }, ...prev]
      return next.slice(0, MAX_LOG)
    })
  }, [])

  // Derive synthetic log events from diff between polls
  const deriveLogs = useCallback((fresh, prev) => {
    if (!prev) return

    const fr = fresh.metrics
    const pr = prev.metrics

    const newReqs = fr.total_requests_processed - pr.total_requests_processed
    if (newReqs > 0) {
      // Figure out which backend served (round-robin impression)
      const servers = Object.entries(fresh.backends)
      const healthy = servers.filter(([, s]) => s.healthy)
      if (healthy.length > 0) {
        const pick = healthy[pollCount.current % healthy.length]
        pushLog(`ROUTE  +${newReqs} req → ${pick[0]}  (${pick[1].port})`, 'route')
      }
    }

    const newBlocks = fr.total_blocked_connections - pr.total_blocked_connections
    if (newBlocks > 0) {
      pushLog(`BOUNCER  ${newBlocks} connection(s) rate-limited [429]`, 'block')
    }

    const byteDiff = fr.aggregate_bytes_transferred - pr.aggregate_bytes_transferred
    if (byteDiff > 0) {
      pushLog(`RELAY   +${fmtBytes(byteDiff)} transferred`, 'route')
    }

    // Health state changes
    Object.entries(fresh.backends).forEach(([sid, srv]) => {
      const old = prev.backends?.[sid]
      if (old && old.healthy && !srv.healthy) {
        pushLog(`HEALTH  ${sid} → OFFLINE  (fail #${srv.consecutive_failures})`, 'warn')
      }
      if (old && !old.healthy && srv.healthy) {
        pushLog(`HEALTH  ${sid} → RECOVERED  ✓`, 'health')
      }
    })

    // Pool changes
    Object.entries(fresh.pools).forEach(([sid, depth]) => {
      const oldDepth = prev.pools?.[sid]
      if (oldDepth !== undefined && oldDepth !== depth) {
        pushLog(`POOL    ${sid} warm sockets: ${oldDepth} → ${depth}`, 'pool')
      }
    })
  }, [pushLog])

  useEffect(() => {
    let mounted = true
    let timerId

    async function poll() {
      if (!mounted) return
      pollCount.current += 1
      try {
        const res = await fetch(API_URL)
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const json = await res.json()

        if (mounted) {
          deriveLogs(json, prevData.current)
          prevData.current = json
          setData(json)
          setError(null)
        }
      } catch (e) {
        if (mounted) {
          setError(e.message)
          if (pollCount.current === 1 || pollCount.current % 5 === 0) {
            pushLog(`SYS  Cannot reach ${API_URL} — ${e.message}`, 'warn')
          }
        }
      } finally {
        if (mounted) timerId = setTimeout(poll, POLL_MS)
      }
    }

    poll()
    pushLog('SYS  Janus dashboard initialised — polling every 1000ms', 'health')

    return () => {
      mounted = false
      clearTimeout(timerId)
    }
  }, [deriveLogs, pushLog])

  return { data, error, logs, clock }
}

export { fmt, fmtBytes }
