// KDE density cloud computation using D3 contour density.
// d3 is loaded as a CDN global (window.d3) — not bundled.
// store.corpKde shape: { [corpName]: { [h]: polygons[] } }
import { store } from './store.js'
import { tradColor } from '../shared/constants.js'

function kdePolygons(pts, color, bandwidth, thresholds, xScale, yScale) {
  const density = d3.contourDensity()
    .x(d => xScale(d.x))
    .y(d => yScale(d.y))
    .size([200, 200])
    .bandwidth(bandwidth)
    .thresholds(thresholds)(pts)
  if (!density.length) return []

  const maxVal = density[density.length - 1].value
  const minThresh = maxVal * 0.08
  const result = []
  for (const contour of density) {
    if (contour.value < minThresh) continue
    const alpha = 0.04 + 0.12 * (contour.value / maxVal)
    for (const ring of contour.coordinates) {
      result.push({
        polygon: ring.map(coords =>
          coords.map(([gx, gy]) => [xScale.invert(gx), yScale.invert(gy)])
        ),
        color,
        alpha,
      })
    }
  }
  return result
}

export function _computeKDE() {
  // Build global x/y extent from all points
  let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity
  for (const p of store.rawPoints) {
    if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x
    if (p.y < yMin) yMin = p.y; if (p.y > yMax) yMax = p.y
  }

  const GRID = 200
  const xScale = d3.scaleLinear().domain([xMin, xMax]).range([0, GRID])
  const yScale = d3.scaleLinear().domain([yMin, yMax]).range([0, GRID])

  // Group points by (corpName, h)
  const byCorpusHeight = {}
  for (const p of store.rawPoints) {
    const corpName = store.rawMapData.corpora[p.ci]
    if (!byCorpusHeight[corpName]) byCorpusHeight[corpName] = {}
    if (!byCorpusHeight[corpName][p.h]) byCorpusHeight[corpName][p.h] = []
    byCorpusHeight[corpName][p.h].push(p)
  }

  store.corpKde = {}
  for (const [corpName, byH] of Object.entries(byCorpusHeight)) {
    const ti = store.rawMapData.trad_of_corpus[store.rawMapData.corpora.indexOf(corpName)]
    const color = tradColor(store.rawMapData.traditions[ti])
    store.corpKde[corpName] = {}
    for (const [hStr, pts] of Object.entries(byH)) {
      const h = parseInt(hStr)
      // Use tighter bandwidth for leaf-level (verse), looser for higher levels
      const bw = h === 0 ? 5 : 8
      const thresh = h === 0 ? 3 : 4
      store.corpKde[corpName][h] = kdePolygons(pts, color, bw, thresh, xScale, yScale)
    }
  }
}
