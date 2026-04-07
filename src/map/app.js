import { tradColor, shortName } from '../shared/constants.js'
import {
  _buildTradState, _recomputeActive,
  showAll, hideAll,
  toggleCorpusScatter, toggleCorpusLabels, toggleCorpusKde,
  toggleHeight, toggleSolo, isSoloed,
  onSliderMinChange, onSliderMaxChange, applySlider,
  levelName,
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

    // Map data
    mapData: null,
    runMeta: null,
    totalPoints: 0,

    // Tradition/corpus/height filter tree
    tradState: {},

    // Solo state
    soloCorpus: null,

    // Height range slider
    sliderMin: 0,
    sliderMax: 0,
    sliderPending: false,
    sliderLastMoved: 'max',
    globalMaxHeight: 0,

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

    get tradNames() {
      return Object.keys(this.tradState).sort()
    },

    // Shared helpers
    tradColor,
    shortName,

    // Filter methods
    _buildTradState,
    _recomputeActive,
    showAll,
    hideAll,
    toggleCorpusScatter,
    toggleCorpusLabels,
    toggleCorpusKde,
    toggleHeight,
    toggleSolo,
    isSoloed,
    onSliderMinChange,
    onSliderMaxChange,
    applySlider,
    levelName,

    // Render / layer methods
    render,
    _traditionCentroids,
    _corpusCentroids,

    // KDE
    _computeKDE,

    // deck.gl
    _initDeck,
    _onHover,
    _onClick,
    fitResults,
    zoomToResult,
    hoverResult,
    unhoverResult,
    selectResult,
    clearResults,

    // Search
    _visibleCorpora,
    doPassageSearch,
    doTextSearch,
    doSimilarSearch,

    // Autocomplete
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

        // Compute global max height for the slider
        const allHeights = Object.values(data.corpus_levels)
          .flatMap(levels => Object.keys(levels).map(Number))
        this.globalMaxHeight = allHeights.length > 0 ? Math.max(...allHeights) : 0
        this.sliderMin = 0
        this.sliderMax = this.globalMaxHeight

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
