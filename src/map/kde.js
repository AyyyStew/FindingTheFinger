// KDE density cloud computation using D3 contour density.
// d3 is loaded as a CDN global (window.d3) — not bundled.
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
  const byTrad = {}
  const byCorpus = {}
  for (const p of store.rawPoints) {
    if (p.h !== 0) continue
    const tradName = store.rawMapData.traditions[p.ti]
    const corpName = store.rawMapData.corpora[p.ci]
    if (!byTrad[tradName]) byTrad[tradName] = []
    if (!byCorpus[corpName]) byCorpus[corpName] = []
    byTrad[tradName].push(p)
    byCorpus[corpName].push(p)
  }

  let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity
  for (const p of store.rawPoints) {
    if (p.h !== 0) continue
    if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x
    if (p.y < yMin) yMin = p.y; if (p.y > yMax) yMax = p.y
  }

  const GRID = 200
  const xScale = d3.scaleLinear().domain([xMin, xMax]).range([0, GRID])
  const yScale = d3.scaleLinear().domain([yMin, yMax]).range([0, GRID])

  store.tradKde = {}
  for (const [tradName, pts] of Object.entries(byTrad)) {
    store.tradKde[tradName] = kdePolygons(pts, tradColor(tradName), 8, 4, xScale, yScale)
  }

  store.corpKde = {}
  for (const [corpName, pts] of Object.entries(byCorpus)) {
    const ti = store.rawMapData.trad_of_corpus[store.rawMapData.corpora.indexOf(corpName)]
    const color = tradColor(store.rawMapData.traditions[ti])
    store.corpKde[corpName] = kdePolygons(pts, color, 5, 3, xScale, yScale)
  }
}
