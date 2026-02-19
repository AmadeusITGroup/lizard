import React from 'react'

/**
 * Legend for anomaly score (0..1) with optional threshold tick.
 * Props:
 *   - title: string (legend title)
 *   - thresholdPct: number (0..1, position of threshold tick)
 *   - thresholdLabel: string (optional, e.g."z_thr=3.0" or "score≥0.82")
 */
export default function Legend({
  title = "Anomaly score",
  thresholdPct,
  thresholdLabel
}: {
  title?: string,
  thresholdPct?: number,
  thresholdLabel?: string
}) {
  return (
    <div style={{
      position: 'relative',
      width: 180,
      height: 38,
      background: 'rgba(255,255,255,0.92)',
      borderRadius: 8,
      boxShadow: '0 2px 8px rgba(0,0,0,0.06)',
      padding: '8px 12px',
      fontSize: 13
    }}>
      <div style={{ fontWeight: 700, marginBottom: 4 }}>{title}</div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, position: 'relative', height: 16 }}>
        <span style={{ fontSize: 12, opacity: 0.7 }}>0</span>
        <div style={{
          position: 'relative',
          height: 10,
          width: 120,
          background: 'linear-gradient(90deg,#1a6,#6cf,#fd0,#f40,#a00)',
          borderRadius: 4
        }}>
          {typeof thresholdPct === 'number' && (
            <div style={{
              position: 'absolute',
              left: `${Math.max(0, Math.min(1, thresholdPct)) * 100}%`,
              top: -3,
              width: 2,
              height: 16,
              background: '#111',
              borderRadius: 1
            }} />
          )}
        </div>
        <span style={{ fontSize: 12, opacity: 0.7 }}>1</span>
      </div>
      {typeof thresholdPct === 'number' && (
        <div style={{
          marginTop: 2,
          fontSize: 12,
          opacity: 0.8,
          textAlign: 'center'
        }}>
          {thresholdLabel
            ? thresholdLabel
            : `threshold: ${thresholdPct.toFixed(2)}`}
        </div>
      )}
    </div>
  )
}