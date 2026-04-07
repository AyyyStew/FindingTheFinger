import {
  tradColor, tradEmoji, corpusEmoji, shortName,
  tradBadgeStyle, corpusBadgeStyle,
} from '../shared/constants.js'
import { _acFetch, acFocus, acInput, acKeydown, acSelect } from './autocomplete.js'

export function app() {
  return {
    // State
    corpora: [],
    tradEnabled: {},
    corpusEnabled: {},

    activeTab: 'verse',
    selectedCorpus: '',
    refInput: '',
    queryInput: '',
    PAGE_SIZE: 50,
    cards: [],
    apiOffset: 0,
    apiHasMore: true,
    lastParams: null,

    loading: false,
    errorMsg: '',
    searched: false,
    sourceVerse: null,

    acItems: [],
    acActive: -1,
    acOpen: false,
    acValid: false,
    acSelectedNode: null,
    targetHeight: null,
    _acTimer: null,

    // Pure helpers from shared constants — assigned directly as methods
    // so Alpine templates can call them as e.g. tradColor(t)
    tradColor,
    tradEmoji,
    corpusEmoji,
    shortName,
    tradBadgeStyle,
    corpusBadgeStyle,

    // Autocomplete methods
    _acFetch,
    acFocus,
    acInput,
    acKeydown,
    acSelect,

    get traditions() {
      return [...new Set(this.corpora.map(c => c.tradition))].sort()
    },

    get hasMore() {
      return this.apiHasMore
    },

    async init() {
      try {
        const data = await fetch('/api/v1/corpora').then(r => r.json())
        this.corpora = data
        if (data.length) this.selectedCorpus = data[0].corpus
        const te = {}, ce = {}
        data.forEach(c => { te[c.tradition] = true; ce[c.corpus] = true })
        this.tradEnabled = te
        this.corpusEnabled = ce
      } catch (e) {
        console.error('Failed to load corpora', e)
      }
    },

    isLong(text) {
      return text && text.split(/\s+/).length > 200
    },

    corpusLevels(corpusName) {
      const c = this.corpora.find(c => c.corpus === corpusName)
      return c?.levels || { 0: 'Verse' }
    },

    toggleTradition(t) {
      const next = !this.tradEnabled[t]
      this.tradEnabled = { ...this.tradEnabled, [t]: next }
      const updated = { ...this.corpusEnabled }
      this.corpora.filter(c => c.tradition === t).forEach(c => { updated[c.corpus] = next })
      this.corpusEnabled = updated
    },

    toggleCorpus(corpus, tradition) {
      const next = !this.corpusEnabled[corpus]
      this.corpusEnabled = { ...this.corpusEnabled, [corpus]: next }
      if (next && tradition && !this.tradEnabled[tradition]) {
        this.tradEnabled = { ...this.tradEnabled, [tradition]: true }
      }
    },

    async doVerseSearch() {
      if (!this.acValid || !this.acSelectedNode) return
      this.acOpen = false
      this.lastParams = {
        type: 'similar',
        unitId: this.acSelectedNode.id,
        targetHeight: this.targetHeight,
      }
      await this.startSearch()
    },

    async doTextSearch() {
      const q = this.queryInput.trim()
      if (!q) return
      this.sourceVerse = null
      this.lastParams = { type: 'text', q }
      await this.startSearch()
    },

    async startSearch() {
      this.cards = []
      this.apiOffset = 0
      this.apiHasMore = true
      this.errorMsg = ''
      this.searched = true
      this.loading = true
      try {
        await this._loadMore()
      } catch (e) {
        this.errorMsg = e.message
      } finally {
        this.loading = false
        this._scrollTo('results-anchor', 800)
      }
    },

    async loadMore() {
      this.loading = true
      try {
        await this._loadMore()
      } catch (e) {
        this.errorMsg = e.message
      } finally {
        this.loading = false
      }
    },

    async _loadMore() {
      if (!this.apiHasMore) return
      const batch = await this._fetchBatch(this.apiOffset)
      this.apiOffset += batch.length
      this.apiHasMore = batch.length === this.PAGE_SIZE
      this.cards = [...this.cards, ...batch]
    },

    async _fetchBatch(offset) {
      const p = this.lastParams
      const enabledCorpora = Object.entries(this.corpusEnabled)
        .filter(([, v]) => v)
        .map(([k]) => k)

      let url, params
      if (p.type === 'similar') {
        params = new URLSearchParams({ limit: this.PAGE_SIZE, offset })
        if (p.targetHeight !== null && p.targetHeight !== undefined)
          params.set('target_height', p.targetHeight)
        enabledCorpora.forEach(c => params.append('corpora', c))
        url = `/api/v1/similar/${p.unitId}?${params}`
      } else {
        params = new URLSearchParams({ q: p.q, limit: this.PAGE_SIZE, offset })
        enabledCorpora.forEach(c => params.append('corpora', c))
        url = `/api/v1/search?${params}`
      }

      const resp = await fetch(url)
      if (!resp.ok) throw new Error((await resp.json()).detail || resp.statusText)
      return resp.json()
    },

    _scrollTo(id, duration = 600) {
      const el = document.getElementById(id)
      if (!el) return
      const start = window.scrollY
      const target = el.getBoundingClientRect().top + start - 16
      const diff = target - start
      let startTime = null
      const ease = t => t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2
      function step(ts) {
        if (!startTime) startTime = ts
        const t = Math.min((ts - startTime) / duration, 1)
        window.scrollTo(0, start + diff * ease(t))
        if (t < 1) requestAnimationFrame(step)
      }
      requestAnimationFrame(step)
    },
  }
}
