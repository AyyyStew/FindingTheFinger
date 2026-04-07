// Search logic for the map page (passage, text, and similar searches).
import { store } from './store.js'

export function _visibleCorpora() {
  if (!store.rawMapData) return []
  const visible = []
  for (let ci = 0; ci < store.rawMapData.corpora.length; ci++) {
    const corpName = store.rawMapData.corpora[ci]
    const tradName = store.rawMapData.traditions[store.rawMapData.trad_of_corpus[ci]]
    const ts = this.tradState[tradName]
    if (!ts?.visible) continue
    if (ts.corpora[corpName]?.visible) visible.push(corpName)
  }
  return visible
}

export async function doPassageSearch() {
  if (!this.acValid || !this.acSelectedNode) return
  await this.doSimilarSearch(this.acSelectedNode.id, this.similarHeight)
}

export async function doTextSearch() {
  const q = this.queryInput.trim()
  if (!q) return
  this.searchLoading = true
  this.searched = true
  try {
    const params = new URLSearchParams({ q, limit: 50, offset: 0 })
    for (const c of this._visibleCorpora()) params.append('corpora', c)
    const rows = await fetch('/api/v1/search?' + params).then(r => {
      if (!r.ok) throw new Error(r.statusText)
      return r.json()
    })
    this._queryPoint = null
    this.results = rows
    this.highlightIds = new Set(rows.map(r => r.unit_id).filter(id => store.deckActiveIds.has(id)))
    this.render('text-search')
    if (rows.length > 0) this.fitResults()
  } catch (e) {
    console.error(e)
  } finally {
    this.searchLoading = false
  }
}

export async function doSimilarSearch(unit_id, targetHeight = null) {
  this.searchLoading = true
  this.searched = true
  this.searchTab = 'passage'
  try {
    const params = new URLSearchParams({ limit: 50, offset: 0 })
    if (targetHeight !== null) params.set('target_height', targetHeight)
    for (const c of this._visibleCorpora()) params.append('corpora', c)
    const rows = await fetch(`/api/v1/similar/${unit_id}?${params}`).then(r => {
      if (!r.ok) throw new Error(r.statusText)
      return r.json()
    })

    const srcPoint = store.rawPoints.find(p => p.id === unit_id)
    if (srcPoint) {
      this.selectedCorpus = store.rawMapData.corpora[srcPoint.ci]
      this.refInput = srcPoint.label
      this.acValid = true
      this.acSelectedNode = { id: unit_id, label: srcPoint.label, height: srcPoint.h }
      this.similarHeight = targetHeight !== null ? targetHeight : srcPoint.h
    }

    this._queryPoint = srcPoint || null
    this.results = rows
    this.highlightIds = new Set(rows.map(r => r.unit_id).filter(id => store.deckActiveIds.has(id)))
    this.render('similar-search')
    if (rows.length > 0) this.fitResults()
  } catch (e) {
    console.error(e)
  } finally {
    this.searchLoading = false
  }
}
