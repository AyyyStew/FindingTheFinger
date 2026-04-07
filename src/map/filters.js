// Filter state management for the map's tradition/corpus/height tree.
// All functions use `this` and are assigned as Alpine component methods.
import { store } from './store.js'

export function _buildTradState(data) {
  const tradCorpora = {}
  for (let ci = 0; ci < data.corpora.length; ci++) {
    const trad = data.traditions[data.trad_of_corpus[ci]]
    if (!tradCorpora[trad]) tradCorpora[trad] = []
    tradCorpora[trad].push(ci)
  }

  this.tradState = {}
  for (const [trad, corpusIndices] of Object.entries(tradCorpora)) {
    const corporaState = {}
    for (const ci of corpusIndices) {
      const corpName = data.corpora[ci]
      const levels = data.corpus_levels[ci] || {}
      const heights = {}
      for (const h of Object.keys(levels)) heights[parseInt(h)] = parseInt(h) === 0
      if (!(0 in heights)) heights[0] = true
      corporaState[corpName] = { visible: true, expanded: false, heights, kde: false }
    }
    this.tradState[trad] = { visible: true, expanded: false, kde: true, corpora: corporaState }
  }
}

export function _recomputeActive() {
  if (!store.rawMapData) return
  const active = []
  for (let ci = 0; ci < store.rawMapData.corpora.length; ci++) {
    const corpName = store.rawMapData.corpora[ci]
    const tradName = store.rawMapData.traditions[store.rawMapData.trad_of_corpus[ci]]
    const ts = this.tradState[tradName]
    if (!ts?.visible) continue
    const cs = ts.corpora[corpName]
    if (!cs?.visible) continue
    const partition = store.byCorpusHeight[ci] || {}
    for (const [hStr, enabled] of Object.entries(cs.heights)) {
      if (!enabled) continue
      const pts = partition[parseInt(hStr)]
      if (pts) for (const p of pts) active.push(p)
    }
  }
  store.deckActivePoints = active
  store.deckActiveIds = new Set(active.map(p => p.id))
  this._activePoints = active
}

export function toggleAllTraditions() {
  const anyVisible = Object.values(this.tradState).some(ts => ts.visible)
  for (const ts of Object.values(this.tradState)) {
    ts.visible = !anyVisible
    ts.kde = !anyVisible
    for (const cs of Object.values(ts.corpora)) {
      cs.visible = !anyVisible
      cs.kde = !anyVisible
    }
  }
  this._recomputeActive()
  this.render('filter')
}

export function toggleTradition(tradName) {
  const ts = this.tradState[tradName]
  ts.visible = !ts.visible
  for (const cs of Object.values(ts.corpora)) cs.visible = ts.visible
  this._recomputeActive()
  this.render('filter')
}

export function toggleTradKde(tradName) {
  this.tradState[tradName].kde = !this.tradState[tradName].kde
  this.render('filter')
}

export function toggleCorpusKde(tradName, corpName) {
  this.tradState[tradName].corpora[corpName].kde =
    !this.tradState[tradName].corpora[corpName].kde
  this.render('filter')
}

export function toggleCorpus(tradName, corpName) {
  const cs = this.tradState[tradName].corpora[corpName]
  cs.visible = !cs.visible
  const ts = this.tradState[tradName]
  ts.visible = Object.values(ts.corpora).some(c => c.visible)
  this._recomputeActive()
  this.render('filter')
}

export function toggleHeight(tradName, corpName, h) {
  this.tradState[tradName].corpora[corpName].heights[h] =
    !this.tradState[tradName].corpora[corpName].heights[h]
  this._recomputeActive()
  this.render('filter')
}

export function toggleTradExpand(tradName) {
  this.tradState[tradName].expanded = !this.tradState[tradName].expanded
}

export function toggleCorpusExpand(tradName, corpName) {
  this.tradState[tradName].corpora[corpName].expanded =
    !this.tradState[tradName].corpora[corpName].expanded
}

export function tradFullyVisible(tradName) {
  const ts = this.tradState[tradName]
  return ts?.visible && Object.values(ts.corpora).every(c => c.visible)
}

export function levelName(ci, h) {
  const levels = this.mapData?.corpus_levels?.[ci] || {}
  return levels[h] ?? `Level ${h}`
}
