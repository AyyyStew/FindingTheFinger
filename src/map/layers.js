// deck.gl layer construction and render() for the map page.
// deck.gl is loaded as a CDN global (window.deck) — not bundled.
import { store, getPos3D } from './store.js'
import { tradColor, hexToRgb } from '../shared/constants.js'

export function render(source = 'unknown') {
  if (!store.deck || !store.rawMapData) return
  const t0 = performance.now()
  const data = store.rawMapData
  const zoom = store.zoom
  const soloActive = !!this.soloCorpus
  const layers = []

  // ── KDE density clouds (per corpus+level, respects solo) ──────────────────
  {
    const kdePolygons = []
    for (const [tradName, ts] of Object.entries(this.tradState)) {
      for (const [corpName, cs] of Object.entries(ts.corpora)) {
        if (soloActive) {
          const isSoloedCorpus =
            this.soloCorpus.tradName === tradName &&
            this.soloCorpus.corpName === corpName
          if (!isSoloedCorpus) continue
        }
        for (const [hStr, ls] of Object.entries(cs.levels)) {
          const h = parseInt(hStr)
          if (soloActive && this.soloCorpus.height !== null && this.soloCorpus.height !== h) continue
          if (!ls.kde) continue
          const polys = store.corpKde[corpName]?.[h]
          if (polys) for (const p of polys) kdePolygons.push(p)
        }
      }
    }
    if (kdePolygons.length > 0) {
      layers.push(new deck.PolygonLayer({
        id: 'kde',
        data: kdePolygons,
        pickable: false,
        stroked: true,
        filled: true,
        getPolygon: d => d.polygon,
        getFillColor: d => [...hexToRgb(d.color), Math.round(d.alpha * 255)],
        getLineColor: d => [...hexToRgb(d.color), 80],
        getLineWidth: 0.5,
        lineWidthUnits: 'pixels',
      }))
    }
  }

  // ── Scatter plot (_recomputeActive handles all filtering) ──────────────────
  if (store.deckActivePoints.length > 0) {
    layers.push(new deck.ScatterplotLayer({
      id: 'points',
      data: store.deckActivePoints,
      pickable: true,
      opacity: 0.5,
      stroked: false,
      filled: true,
      radiusMinPixels: 1,
      radiusMaxPixels: 6,
      getRadius: d => d.h === 0 ? 0.08 : 0.18,
      getPosition: getPos3D,
      getFillColor: d => {
        const rgb = hexToRgb(tradColor(data.traditions[d.ti]))
        if (this.highlightIds.size > 0) {
          return this.highlightIds.has(d.id) ? [...rgb, 255] : [...rgb, 40]
        }
        return [...rgb, d.h === 0 ? 180 : 230]
      },
      updateTriggers: {
        getFillColor: [this.highlightIds.size, this.highlightIds],
        getRadius: [],
      },
    }))
  }

  // ── Labels ────────────────────────────────────────────────────────────────
  {
    const labelData = []

    // Tradition centroid labels — computed from all active points
    if (zoom >= store.initialZoom - 1) {
      for (const d of this._traditionCentroids(data))
        labelData.push({ ...d, size: 14, alpha: 210 })
    }

    // Corpus/level centroid labels — per level toggle, respects solo
    const labeledLevels = [] // [{corpName, h}]
    for (const [tradName, ts] of Object.entries(this.tradState)) {
      for (const [corpName, cs] of Object.entries(ts.corpora)) {
        if (soloActive) {
          const isSoloedCorpus =
            this.soloCorpus.tradName === tradName &&
            this.soloCorpus.corpName === corpName
          if (!isSoloedCorpus) continue
        }
        for (const [hStr, ls] of Object.entries(cs.levels)) {
          const h = parseInt(hStr)
          if (soloActive && this.soloCorpus.height !== null && this.soloCorpus.height !== h) continue
          if (ls.labels) labeledLevels.push({ corpName, h })
        }
      }
    }
    console.log('[render labels] labeledLevels=', JSON.stringify(labeledLevels))
    if (labeledLevels.length > 0) {
      const centroids = this._corpusCentroids(data, labeledLevels)
      console.log('[render labels] centroids=', JSON.stringify(centroids))
      for (const d of centroids)
        labelData.push({ ...d, size: 11, alpha: 175 })
    }

    if (labelData.length > 0) {
      layers.push(new deck.TextLayer({
        id: 'labels',
        data: labelData,
        pickable: false,
        getPosition: getPos3D,
        getText: d => d.name,
        getColor: d => [...hexToRgb(d.color), d.alpha],
        getSize: d => d.size,
        fontWeight: 700,
        background: true,
        getBackgroundColor: [15, 17, 23, 170],
        backgroundPadding: [3, 2],
        characterSet: 'auto',
        fontFamily: "'Segoe UI', system-ui, sans-serif",
        getTextAnchor: 'middle',
        getAlignmentBaseline: 'center',
        updateTriggers: {
          getColor: [zoom, labeledLevels.length],
          getSize: [zoom, labeledLevels.length],
        },
      }))
    }
  }

  // ── Search constellation ──────────────────────────────────────────────────
  if (this._queryPoint && this.results.length > 0) {
    const lineData = this.results
      .map(r => ({ src: this._queryPoint, tgt: store.pointById.get(r.unit_id) }))
      .filter(d => d.tgt && store.deckActiveIds.has(d.tgt.id))

    layers.push(new deck.LineLayer({
      id: 'search-lines',
      data: lineData,
      pickable: false,
      getSourcePosition: d => [d.src.x, d.src.y],
      getTargetPosition: d => [d.tgt.x, d.tgt.y],
      getColor: d => [...hexToRgb(tradColor(data.traditions[d.tgt.ti])), 200],
      getWidth: 1.5,
      widthUnits: 'pixels',
    }))

    layers.push(new deck.ScatterplotLayer({
      id: 'query-point',
      data: [this._queryPoint],
      pickable: false,
      getPosition: getPos3D,
      getFillColor: [255, 255, 255, 220],
      getLineColor: [108, 140, 255, 255],
      stroked: true,
      filled: true,
      getRadius: 0.15,
      radiusMinPixels: 5,
      radiusMaxPixels: 12,
      getLineWidth: 2,
      lineWidthUnits: 'pixels',
    }))
  }

  store.deck.setProps({ layers })

  const dt = performance.now() - t0
  this._renderCount++
  this._lastRenderMs = dt
  console.debug(
    `[render #${this._renderCount} ← ${source}] ${dt.toFixed(1)}ms | ` +
    `${layers.length} layers | ${this._activePoints.length} pts | zoom=${zoom.toFixed(2)}`
  )
}

// Tradition centroids — computed from all currently active points
export function _traditionCentroids(data) {
  const byTrad = {}
  for (const p of store.deckActivePoints) {
    const t = data.traditions[p.ti]
    if (!byTrad[t]) byTrad[t] = { xs: [], ys: [] }
    byTrad[t].xs.push(p.x)
    byTrad[t].ys.push(p.y)
  }
  return Object.entries(byTrad).map(([name, { xs, ys }]) => ({
    name,
    color: tradColor(name),
    x: xs.reduce((a, b) => a + b, 0) / xs.length,
    y: ys.reduce((a, b) => a + b, 0) / ys.length,
  }))
}

// labeledLevels: [{corpName, h}] — return one label per point at those levels.
// Each point already has its own label (book name, chapter name, etc.) and position.
export function _corpusCentroids(data, labeledLevels) {
  const wanted = new Set(
    labeledLevels.filter(({ h }) => h > 0).map(({ corpName, h }) => `${corpName}__${h}`)
  )
  const results = []
  for (const p of store.rawPoints) {
    const corpName = data.corpora[p.ci]
    if (!wanted.has(`${corpName}__${p.h}`)) continue
    results.push({
      name: p.label,
      color: tradColor(data.traditions[p.ti]),
      x: p.x,
      y: p.y,
    })
  }
  return results
}
