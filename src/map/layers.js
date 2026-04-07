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

  // ── KDE density clouds (per-corpus, respects solo) ────────────────────────
  {
    const kdePolygons = []
    for (const [tradName, ts] of Object.entries(this.tradState)) {
      for (const [corpName, cs] of Object.entries(ts.corpora)) {
        if (soloActive && !this.isSoloed(tradName, corpName)) continue
        if (cs.kde && store.corpKde[corpName]) {
          for (const p of store.corpKde[corpName]) kdePolygons.push(p)
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

  // ── Scatter plot (_recomputeActive already handles scatter + solo filtering) ─
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

    // Tradition centroid labels — always on when zoomed in enough
    if (zoom >= store.initialZoom - 1) {
      for (const d of this._traditionCentroids(data))
        labelData.push({ ...d, size: 14, alpha: 210 })
    }

    // Corpus centroid labels — per-corpus toggle, respects solo
    const labeledCorpora = new Set()
    for (const [tradName, ts] of Object.entries(this.tradState)) {
      for (const [corpName, cs] of Object.entries(ts.corpora)) {
        if (soloActive && !this.isSoloed(tradName, corpName)) continue
        if (cs.labels) labeledCorpora.add(corpName)
      }
    }
    if (labeledCorpora.size > 0) {
      for (const d of this._corpusCentroids(data, labeledCorpora))
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
        fontFamily: "'Segoe UI', system-ui, sans-serif",
        getTextAnchor: 'middle',
        getAlignmentBaseline: 'center',
        updateTriggers: {
          getColor: [zoom, labeledCorpora.size],
          getSize: [zoom, labeledCorpora.size],
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

export function _traditionCentroids(data) {
  const byTrad = {}
  for (const p of store.deckActivePoints) {
    if (p.h !== 0) continue
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

// filteredCorpora: Set<corpName> | null — if provided, only include those corpora
export function _corpusCentroids(data, filteredCorpora = null) {
  const byCorpus = {}
  for (const p of store.deckActivePoints) {
    if (p.h !== 0) continue
    const c = data.corpora[p.ci]
    if (filteredCorpora && !filteredCorpora.has(c)) continue
    if (!byCorpus[c]) byCorpus[c] = { xs: [], ys: [], ti: p.ti }
    byCorpus[c].xs.push(p.x)
    byCorpus[c].ys.push(p.y)
  }
  return Object.entries(byCorpus).map(([name, { xs, ys, ti }]) => ({
    name: this.shortName(name),
    color: tradColor(data.traditions[ti]),
    x: xs.reduce((a, b) => a + b, 0) / xs.length,
    y: ys.reduce((a, b) => a + b, 0) / ys.length,
  }))
}
