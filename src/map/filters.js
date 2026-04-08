// Filter state management for the map's tradition/corpus/level tree.
// Each level (h) within a corpus is an independent entity with scatter/labels/kde.
// Corpus controls are master convenience toggles — truth lives at the level.
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
      const levelState = {}
      for (const h of Object.keys(levels)) {
        const hInt = parseInt(h)
        levelState[hInt] = { scatter: hInt === 0, labels: false, kde: false }
      }
      if (!(0 in levelState)) levelState[0] = { scatter: true, labels: false, kde: false }
      corporaState[corpName] = { collapsed: false, levels: levelState, aggregate: { scatter: false, labels: false } }
    }
    this.tradState[trad] = { collapsed: false, corpora: corporaState }
  }
}

export function _recomputeActive() {
  if (!store.rawMapData) return
  const soloActive = !!this.soloCorpus
  const active = []

  for (let ci = 0; ci < store.rawMapData.corpora.length; ci++) {
    const corpName = store.rawMapData.corpora[ci]
    const tradName = store.rawMapData.traditions[store.rawMapData.trad_of_corpus[ci]]
    const ts = this.tradState[tradName]
    if (!ts) continue
    const cs = ts.corpora[corpName]
    if (!cs) continue

    const isSoloedCorpus = soloActive &&
      this.soloCorpus.tradName === tradName &&
      this.soloCorpus.corpName === corpName

    if (soloActive && !isSoloedCorpus) continue

    const partition = store.byCorpusHeight[ci] || {}
    for (const [hStr, ls] of Object.entries(cs.levels)) {
      const h = parseInt(hStr)
      if (soloActive && this.soloCorpus.height !== null && this.soloCorpus.height !== h) continue
      if (!ls.scatter) continue
      const pts = partition[h]
      if (pts) for (const p of pts) active.push(p)
    }
  }

  store.deckActivePoints = active
  store.deckActiveIds = new Set(active.map(p => p.id))
  this._activePoints = active
}

// ── Global helpers (apply to currently visible/scatter-on levels) ─────────────

export function showAll() {
  for (const ts of Object.values(this.tradState)) {
    for (const cs of Object.values(ts.corpora)) {
      for (const ls of Object.values(cs.levels)) ls.scatter = true
    }
  }
  this.soloCorpus = null
  this._recomputeActive()
  this.render('show-all')
}

export function hideAll() {
  for (const ts of Object.values(this.tradState)) {
    for (const cs of Object.values(ts.corpora)) {
      for (const ls of Object.values(cs.levels)) ls.scatter = false
    }
  }
  this.soloCorpus = null
  this._recomputeActive()
  this.render('hide-all')
}

export function toggleAllPoints() {
  const anyOn = Object.values(this.tradState).some(ts =>
    Object.values(ts.corpora).some(cs =>
      Object.values(cs.levels).some(ls => ls.scatter)))
  const newVal = !anyOn
  for (const ts of Object.values(this.tradState)) {
    for (const cs of Object.values(ts.corpora)) {
      for (const ls of Object.values(cs.levels)) ls.scatter = newVal
    }
  }
  this.soloCorpus = null
  this._recomputeActive()
  this.render('toggle-all-points')
}

export function toggleAllLabels() {
  const anyOn = Object.values(this.tradState).some(ts =>
    Object.values(ts.corpora).some(cs =>
      Object.values(cs.levels).some(ls => ls.labels)))
  const newVal = !anyOn
  for (const ts of Object.values(this.tradState))
    for (const cs of Object.values(ts.corpora))
      for (const ls of Object.values(cs.levels)) ls.labels = newVal
  this.render('toggle-all-labels')
}

export function toggleAllKde() {
  const anyOn = Object.values(this.tradState).some(ts =>
    Object.values(ts.corpora).some(cs =>
      Object.values(cs.levels).some(ls => ls.kde)))
  const newVal = !anyOn
  for (const ts of Object.values(this.tradState))
    for (const cs of Object.values(ts.corpora))
      for (const ls of Object.values(cs.levels)) ls.kde = newVal
  this.render('toggle-all-kde')
}

// ── Tradition helpers ─────────────────────────────────────────────────────────

export function toggleTradScatter(tradName) {
  const ts = this.tradState[tradName]
  if (!ts) return
  const anyOn = Object.values(ts.corpora).some(cs => cs.aggregate.scatter)
  const newVal = !anyOn
  for (const cs of Object.values(ts.corpora)) cs.aggregate.scatter = newVal
  this.render('trad-scatter')
}

export function toggleTradLabels(tradName) {
  const ts = this.tradState[tradName]
  const anyOn = Object.values(ts.corpora).some(cs => cs.aggregate.labels)
  const newVal = !anyOn
  for (const cs of Object.values(ts.corpora)) cs.aggregate.labels = newVal
  this.render('trad-labels')
}

export function toggleTradKde(tradName) {
  const ts = this.tradState[tradName]
  const anyOn = Object.values(ts.corpora).some(cs => cs.levels[0]?.kde)
  const newVal = !anyOn
  for (const cs of Object.values(ts.corpora)) {
    if (cs.levels[0]) cs.levels[0].kde = newVal
  }
  this.render('trad-kde')
}

// ── Collapse toggles ──────────────────────────────────────────────────────────

export function toggleTradCollapse(tradName) {
  this.tradState[tradName].collapsed = !this.tradState[tradName].collapsed
}

export function toggleCorpusCollapse(tradName, corpName) {
  this.tradState[tradName].corpora[corpName].collapsed =
    !this.tradState[tradName].corpora[corpName].collapsed
}

// ── Corpus master controls (convenience — sets all levels) ────────────────────

export function corpusMasterScatterActive(tradName, corpName) {
  return this.tradState[tradName]?.corpora[corpName]?.aggregate?.scatter ?? false
}

export function corpusMasterLabelsActive(tradName, corpName) {
  return this.tradState[tradName]?.corpora[corpName]?.aggregate?.labels ?? false
}

export function corpusMasterKdeActive(tradName, corpName) {
  return this.tradState[tradName]?.corpora[corpName]?.levels[0]?.kde ?? false
}

export function toggleCorpusMasterScatter(tradName, corpName) {
  const cs = this.tradState[tradName].corpora[corpName]
  cs.aggregate.scatter = !cs.aggregate.scatter
  this.render('corpus-master-scatter')
}

export function toggleCorpusMasterLabels(tradName, corpName) {
  const cs = this.tradState[tradName].corpora[corpName]
  cs.aggregate.labels = !cs.aggregate.labels
  this.render('corpus-master-labels')
}

export function toggleCorpusMasterKde(tradName, corpName) {
  const cs = this.tradState[tradName].corpora[corpName]
  if (cs.levels[0]) cs.levels[0].kde = !cs.levels[0].kde
  this.render('corpus-master-kde')
}

// ── Level controls ────────────────────────────────────────────────────────────

export function toggleLevelScatter(tradName, corpName, h) {
  this.tradState[tradName].corpora[corpName].levels[h].scatter =
    !this.tradState[tradName].corpora[corpName].levels[h].scatter
  this._recomputeActive()
  this.render('level-scatter')
}

export function toggleLevelLabels(tradName, corpName, h) {
  this.tradState[tradName].corpora[corpName].levels[h].labels =
    !this.tradState[tradName].corpora[corpName].levels[h].labels
  this.render('level-labels')
}

export function toggleLevelKde(tradName, corpName, h) {
  this.tradState[tradName].corpora[corpName].levels[h].kde =
    !this.tradState[tradName].corpora[corpName].levels[h].kde
  this.render('level-kde')
}

// ── Solo ──────────────────────────────────────────────────────────────────────

export function toggleSolo(tradName, corpName, height = null) {
  const alreadySoloed =
    this.soloCorpus?.tradName === tradName &&
    this.soloCorpus?.corpName === corpName &&
    this.soloCorpus?.height === height
  this.soloCorpus = alreadySoloed ? null : { tradName, corpName, height }
  this._recomputeActive()
  this.render('solo')
}

export function isSoloed(tradName, corpName, height = null) {
  return this.soloCorpus?.tradName === tradName &&
    this.soloCorpus?.corpName === corpName &&
    this.soloCorpus?.height === height
}

// ── Height range slider ───────────────────────────────────────────────────────

export function onSliderMinChange() {
  if (this.sliderMin > this.sliderMax) this.sliderMin = this.sliderMax
  this.sliderPending = true
}

export function onSliderMaxChange() {
  if (this.sliderMax < this.sliderMin) this.sliderMax = this.sliderMin
  this.sliderPending = true
}

export function applySlider() {
  const min = this.sliderMin
  const max = this.sliderMax
  for (const ts of Object.values(this.tradState)) {
    for (const cs of Object.values(ts.corpora)) {
      const corpMaxH = Math.max(...Object.keys(cs.levels).map(Number))
      const effectiveMax = Math.min(max, corpMaxH)
      const effectiveMin = Math.min(min, effectiveMax)
      for (const h in cs.levels) {
        const hInt = parseInt(h)
        cs.levels[hInt].scatter = hInt >= effectiveMin && hInt <= effectiveMax
      }
    }
  }
  this.sliderPending = false
  this._recomputeActive()
  this.render('slider-apply')
}

// Returns [{trad, names}] showing which level names the current slider would activate per tradition
export function sliderPreview() {
  if (!this.mapData) return []
  const min = this.sliderMin
  const max = this.sliderMax
  const result = []

  for (const tradName of this.tradNames) {
    const ts = this.tradState[tradName]
    if (!ts) continue
    const seen = new Set()

    for (const [corpName, cs] of Object.entries(ts.corpora)) {
      const ci = this.mapData.corpora.indexOf(corpName)
      const corpMaxH = Math.max(...Object.keys(cs.levels).map(Number))
      const effectiveMax = Math.min(max, corpMaxH)
      const effectiveMin = Math.min(min, effectiveMax)
      for (const h in cs.levels) {
        const hInt = parseInt(h)
        if (hInt >= effectiveMin && hInt <= effectiveMax) {
          seen.add(this.levelName(ci, hInt))
        }
      }
    }

    if (seen.size > 0) result.push({ trad: tradName, names: [...seen].join(', ') })
  }
  return result
}

// ── Helpers ───────────────────────────────────────────────────────────────────

export function levelName(ci, h) {
  const levels = this.mapData?.corpus_levels?.[ci] || {}
  return levels[h] ?? `Level ${h}`
}
