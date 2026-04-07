// Map page Alpine component — assembles state and imports methods from focused modules.
import { tradColor, shortName } from '../shared/constants.js'
import {
  _buildTradState, _recomputeActive,
  toggleAllTraditions, toggleTradition, toggleTradKde, toggleCorpusKde,
  toggleCorpus, toggleHeight, toggleTradExpand, toggleCorpusExpand,
  tradFullyVisible, levelName,
} from './filters.js'
import { render, _traditionCentroids, _corpusCentroids } from './layers.js'
import { _computeKDE } from './kde.js'
import {
  _initDeck, _onHover, _onClick,
  fitResults, zoomToResult, hoverResult, unhoverResult, selectResult, clearResults,
} from './deck-init.js'
import { _visibleCorpora, doPassageSearch, doTextSearch, doSimilarSearch } from './search.js'
import { _acFetch, acFocus, acInput, acKeydown, acSelect } from './autocomplete.js'
import { store } from './store.js'

export function mapApp() {
  return {
    // UI state
    loading: true,
    loadMsg: 'Loading map data…',
    leftOpen: false,
    sidebarOpen: false,

    // Map data (Alpine-proxied copies for UI bindings)
    mapData: null,
    runMeta: null,
    totalPoints: 0,

    // Display toggles
    layers: { scatter: true, labels: true, corpusLabels: false },

    // Tradition/corpus/height filter tree
    tradState: {},

    // Search state
    searchTab: 'history',
    corpora: [],
    selectedCorpus: '',
    refInput: '',
    queryInput: '',
    acItems: [],
    acActive: -1,
    acOpen: false,
    acValid: false,
    acSelectedNode: null,
    similarHeight: null,
    _acTimer: null,
    searchLoading: false,
    searched: false,
    results: [],
    activeResult: null,
    highlightIds: new Set(),
    history: [],

    // Perf overlay
    _activePoints: [],
    _queryPoint: null,
    _renderCount: 0,
    _lastRenderMs: 0,

    // Computed — must stay as getters here; spreading loses the getter descriptor
    get anyTraditionVisible() {
      return Object.values(this.tradState).some(ts => ts.visible)
    },
    get tradNames() {
      return Object.keys(this.tradState).sort()
    },

    // Shared helpers assigned as component methods so Alpine templates can call them
    tradColor,
    shortName,

    // Filter methods (filters.js)
    _buildTradState,
    _recomputeActive,
    toggleAllTraditions,
    toggleTradition,
    toggleTradKde,
    toggleCorpusKde,
    toggleCorpus,
    toggleHeight,
    toggleTradExpand,
    toggleCorpusExpand,
    tradFullyVisible,
    levelName,

    // Render / layer methods (layers.js)
    render,
    _traditionCentroids,
    _corpusCentroids,

    // KDE computation (kde.js)
    _computeKDE,

    // deck.gl init + interaction (deck-init.js)
    _initDeck,
    _onHover,
    _onClick,
    fitResults,
    zoomToResult,
    hoverResult,
    unhoverResult,
    selectResult,
    clearResults,

    // Search (search.js)
    _visibleCorpora,
    doPassageSearch,
    doTextSearch,
    doSimilarSearch,

    // Autocomplete (autocomplete.js)
    _acFetch,
    acFocus,
    acInput,
    acKeydown,
    acSelect,

    async init() {
      try {
        const data = await fetch('/api/v1/corpora').then(r => r.json())
        this.corpora = data
        if (data.length) this.selectedCorpus = data[0].corpus
      } catch (e) {
        console.error('Failed to load corpora', e)
      }

      try {
        this.loadMsg = 'Loading map data…'
        const data = await fetch('/api/v1/map').then(r => r.json())

        // Capture raw data BEFORE assigning to Alpine — once Alpine proxies it,
        // arrays passed to deck.gl layers must come from store, not from this.*
        store.rawMapData = data
        store.rawPoints = data.points

        store.byCorpusHeight = {}
        for (const p of store.rawPoints) {
          if (!store.byCorpusHeight[p.ci]) store.byCorpusHeight[p.ci] = {}
          if (!store.byCorpusHeight[p.ci][p.h]) store.byCorpusHeight[p.ci][p.h] = []
          store.byCorpusHeight[p.ci][p.h].push(p)
        }
        store.pointById = new Map(store.rawPoints.map(p => [p.id, p]))

        this.mapData = data
        this.runMeta = data.run
        this.totalPoints = store.rawPoints.filter(p => p.h === 0).length

        this._buildTradState(data)
        this._recomputeActive()

        this.loadMsg = 'Computing density clouds…'
        await this.$nextTick()
        this._computeKDE()

        this.loading = false
        await this.$nextTick()
        this._initDeck()
        this.render('init')
      } catch (e) {
        this.loadMsg = 'Failed to load map data. Has compute_umap.py been run?'
        console.error(e)
      }
    },
  }
}
