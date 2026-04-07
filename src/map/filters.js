// Filter state management for the map's tradition/corpus/height tree.
// Corpuses are the primary objects — traditions are grouping headers only.
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
      corporaState[corpName] = {
        scatter: true,
        labels: false,
        kde: false,
        heights,
      }
    }
    this.tradState[trad] = { visible: true, corpora: corporaState }
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

    const isSolo = soloActive &&
      this.soloCorpus.tradName === tradName &&
      this.soloCorpus.corpName === corpName

    if (soloActive && !isSolo) continue
    if (!soloActive && !cs.scatter) continue

    const partition = store.byCorpusHeight[ci] || {}
    for (const [hStr, enabled] of Object.entries(cs.heights)) {
      if (!enabled) continue
      const pts = partition[parseInt(hStr)]
      if (pts) for (const p of pts) active.push(p)
    }
  }

  // Keep ts.visible in sync (used by label rendering to know which traditions have points)
  for (const [, ts] of Object.entries(this.tradState)) {
    ts.visible = Object.values(ts.corpora).some(cs => cs.scatter)
  }

  store.deckActivePoints = active
  store.deckActiveIds = new Set(active.map(p => p.id))
  this._activePoints = active
}

// ── Global controls ───────────────────────────────────────────────────────────

export function showAll() {
  for (const ts of Object.values(this.tradState)) {
    ts.visible = true
    for (const cs of Object.values(ts.corpora)) cs.scatter = true
  }
  this.soloCorpus = null
  this._recomputeActive()
  this.render('show-all')
}

export function hideAll() {
  for (const ts of Object.values(this.tradState)) {
    ts.visible = false
    for (const cs of Object.values(ts.corpora)) cs.scatter = false
  }
  this.soloCorpus = null
  this._recomputeActive()
  this.render('hide-all')
}

// ── Height range slider ───────────────────────────────────────────────────────

export function onSliderMinChange() {
  // Clamp — handles can meet but not cross
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
      for (const h in cs.heights) {
        const hInt = parseInt(h)
        cs.heights[hInt] = hInt >= min && hInt <= max
      }
    }
  }
  this.sliderPending = false
  this._recomputeActive()
  this.render('slider-apply')
}

// ── Per-corpus toggles ────────────────────────────────────────────────────────

export function toggleCorpusScatter(tradName, corpName) {
  const cs = this.tradState[tradName].corpora[corpName]
  cs.scatter = !cs.scatter
  this.tradState[tradName].visible = Object.values(this.tradState[tradName].corpora)
    .some(c => c.scatter)
  this._recomputeActive()
  this.render('scatter-toggle')
}

export function toggleCorpusLabels(tradName, corpName) {
  this.tradState[tradName].corpora[corpName].labels =
    !this.tradState[tradName].corpora[corpName].labels
  this.render('labels-toggle')
}

export function toggleCorpusKde(tradName, corpName) {
  this.tradState[tradName].corpora[corpName].kde =
    !this.tradState[tradName].corpora[corpName].kde
  this.render('kde-toggle')
}

export function toggleHeight(tradName, corpName, h) {
  this.tradState[tradName].corpora[corpName].heights[h] =
    !this.tradState[tradName].corpora[corpName].heights[h]
  this._recomputeActive()
  this.render('height-toggle')
}

// ── Solo ──────────────────────────────────────────────────────────────────────

export function toggleSolo(tradName, corpName) {
  if (this.soloCorpus?.tradName === tradName && this.soloCorpus?.corpName === corpName) {
    this.soloCorpus = null
  } else {
    this.soloCorpus = { tradName, corpName }
  }
  this._recomputeActive()
  this.render('solo')
}

export function isSoloed(tradName, corpName) {
  return this.soloCorpus?.tradName === tradName && this.soloCorpus?.corpName === corpName
}

// ── Helpers ───────────────────────────────────────────────────────────────────

export function levelName(ci, h) {
  const levels = this.mapData?.corpus_levels?.[ci] || {}
  return levels[h] ?? `Level ${h}`
}
