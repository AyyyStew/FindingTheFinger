// map.js — deck.gl UMAP map page logic

// ---------------------------------------------------------------------------
// Stable accessor functions — defined once so deck.gl sees the same reference
// across render() calls and skips unnecessary GPU re-uploads
// ---------------------------------------------------------------------------
const getPos3D = d => [d.x, d.y, 0];

// ---------------------------------------------------------------------------
// Plain JS data stores for deck.gl — kept outside Alpine's reactive proxy so
// deck.gl never receives an Alpine Proxy as a `data` prop (which violates the
// JS Proxy non-writable/non-configurable property invariant and kills layers).
//
// Rule: anything passed as `data:` to a deck.gl layer must come from here,
// never from `this.*`. Alpine deeply proxies all component state on access.
// ---------------------------------------------------------------------------
let _rawMapData       = null;   // raw API response, never touched by Alpine
let _rawPoints        = [];     // raw points array (same ref as _rawMapData.points)
let _byCorpusHeight   = {};     // ci -> h -> point[]  (raw points, plain objects)
let _pointById        = null;   // Map<id, point>  (raw points)
let _deckActivePoints = [];     // current active subset — passed to ScatterplotLayer
let _tradKde          = {};     // tradName -> [{polygon,color,alpha}]  (computed once)
let _corpKde          = {};     // corpName -> [{polygon,color,alpha}]  (computed once)
let _deck             = null;   // Deck.gl instance — MUST live outside Alpine:
                                // Alpine deep-proxies all component state, which wraps
                                // deck's internal layer cache. deck.gl marks layer.data
                                // non-writable/non-configurable via Object.defineProperty,
                                // so the Proxy invariant fires on every re-render.
let _zoom             = 0;
let _initialZoom      = 0;

// ---------------------------------------------------------------------------
// Tradition colours
// ---------------------------------------------------------------------------
const TRADITION_CONFIG = {
  'Abrahamic':   { color: '#f5c518' },
  'Buddhist':    { color: '#fb923c' },
  'Confucian':   { color: '#34d399' },
  'Dharmic':     { color: '#c084fc' },
  'Norse':       { color: '#93c5fd' },
  'Shinto':      { color: '#f9a8d4' },
  'Sikh':        { color: '#22d3ee' },
  'Taoist':      { color: '#d4d4d8' },
  'Zoroastrian': { color: '#f87171' },
};

function hexToRgb(hex) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return [r, g, b];
}

function tradColor(name) {
  return TRADITION_CONFIG[name]?.color || '#6c8cff';
}

// ---------------------------------------------------------------------------
// Alpine component
// ---------------------------------------------------------------------------
function mapApp() {
  return {
    // ---- UI state ----
    loading:       true,
    loadMsg:       'Loading map data…',
    leftOpen:      false,
    sidebarOpen:   false,

    // ---- Map data ----
    mapData:       null,
    runMeta:       null,
    totalPoints:   0,

    // ---- Global display toggles ----
    layers: {
      scatter:      true,
      labels:       true,
      corpusLabels: false,
    },

    // ---- Tradition/corpus/height filter state ----
    // tradState[tradName] = {
    //   visible: bool,       // master toggle for tradition
    //   expanded: bool,      // tree expanded in panel
    //   corpora: {
    //     [corpusName]: {
    //       visible: bool,
    //       expanded: bool,
    //       heights: { [h]: bool }  // which heights are active for this corpus
    //     }
    //   }
    // }
    tradState: {},

    // ---- Search state ----
    searchTab:      'history',
    corpora:        [],
    selectedCorpus: '',
    refInput:       '',
    queryInput:     '',
    acItems:        [],
    acActive:       -1,
    acOpen:         false,
    acValid:        false,
    _acTimer:       null,
    searchLoading:  false,
    searched:       false,
    results:        [],
    activeResult:   null,
    highlightIds:   new Set(),
    history:        [],

    // _activePoints kept only for the perf overlay count; real data in _deckActivePoints
    _activePoints:  [],

    // ---- Search constellation ----
    _queryPoint:    null,

    // ---- Perf ----
    _renderCount:   0,
    _lastRenderMs:  0,

    // -----------------------------------------------------------------------
    // Init
    // -----------------------------------------------------------------------
    async init() {
      try {
        const data = await fetch('/api/v1/corpora').then(r => r.json());
        this.corpora = data;
        if (data.length) this.selectedCorpus = data[0].corpus;
      } catch(e) { console.error('Failed to load corpora', e); }

      try {
        this.loadMsg = 'Loading map data…';
        const data = await fetch('/api/v1/map').then(r => r.json());

        // ── Capture raw data BEFORE assigning to Alpine ──────────────────────
        // Once we do `this.mapData = data`, Alpine wraps everything in a Proxy.
        // All arrays passed to deck.gl must come from these plain module-level vars.
        _rawMapData = data;
        _rawPoints  = data.points;

        // Build per-corpus-height index from raw points
        _byCorpusHeight = {};
        for (const p of _rawPoints) {
          if (!_byCorpusHeight[p.ci]) _byCorpusHeight[p.ci] = {};
          if (!_byCorpusHeight[p.ci][p.h]) _byCorpusHeight[p.ci][p.h] = [];
          _byCorpusHeight[p.ci][p.h].push(p);
        }

        // O(1) lookup for leaves
        _pointById = new Map(_rawPoints.filter(p => p.h === 0).map(p => [p.id, p]));

        // ── Now assign to Alpine state (for UI bindings) ──────────────────────
        this.mapData     = data;
        this.runMeta     = data.run;
        this.totalPoints = _rawPoints.filter(p => p.h === 0).length;

        // Build tradState from raw data
        this._buildTradState(data);

        // Initial active points (writes to _deckActivePoints)
        this._recomputeActive();

        this.loadMsg = 'Computing density clouds…';
        await this.$nextTick();
        this._computeKDE();  // populates _tradKde and _corpKde from _rawPoints

        this.loading = false;
        await this.$nextTick();
        this._initDeck();
        this.render('init');
      } catch(e) {
        this.loadMsg = 'Failed to load map data. Has compute_umap.py been run?';
        console.error(e);
      }
    },

    // -----------------------------------------------------------------------
    // Build tradState from map data
    // -----------------------------------------------------------------------
    _buildTradState(data) {
      // Group corpora by tradition
      const tradCorpora = {};
      for (let ci = 0; ci < data.corpora.length; ci++) {
        const ti = data.trad_of_corpus[ci];
        const trad = data.traditions[ti];
        if (!tradCorpora[trad]) tradCorpora[trad] = [];
        tradCorpora[trad].push(ci);
      }

      this.tradState = {};
      for (const [trad, corpusIndices] of Object.entries(tradCorpora)) {
        const corporaState = {};
        for (const ci of corpusIndices) {
          const corpName = data.corpora[ci];
          const levels   = data.corpus_levels[ci] || {};
          // Default: only h=0 active
          const heights  = {};
          for (const h of Object.keys(levels)) {
            heights[parseInt(h)] = parseInt(h) === 0;
          }
          // Ensure h=0 exists even if corpus_levels missing entry
          if (!(0 in heights)) heights[0] = true;
          corporaState[corpName] = { visible: true, expanded: false, heights, kde: false };
        }
        this.tradState[trad] = { visible: true, expanded: false, kde: true, corpora: corporaState };
      }
    },

    // -----------------------------------------------------------------------
    // Recompute _activePoints from tradState
    // Called whenever a visibility toggle changes
    // -----------------------------------------------------------------------
    _recomputeActive() {
      if (!_rawMapData) return;

      const active = [];
      for (let ci = 0; ci < _rawMapData.corpora.length; ci++) {
        const corpName = _rawMapData.corpora[ci];
        const tradName = _rawMapData.traditions[_rawMapData.trad_of_corpus[ci]];
        const ts = this.tradState[tradName];
        if (!ts?.visible) continue;
        const cs = ts.corpora[corpName];
        if (!cs?.visible) continue;

        const partition = _byCorpusHeight[ci] || {};
        for (const [hStr, enabled] of Object.entries(cs.heights)) {
          if (!enabled) continue;
          const pts = partition[parseInt(hStr)];
          if (pts) for (const p of pts) active.push(p);
        }
      }
      // Both: _deckActivePoints is a plain array for deck.gl;
      // _activePoints is kept in Alpine only for the perf overlay count.
      _deckActivePoints  = active;
      this._activePoints = active;
    },

    // -----------------------------------------------------------------------
    // Toggle helpers — each calls _recomputeActive + render
    // -----------------------------------------------------------------------
    toggleAllTraditions() {
      const anyVisible = Object.values(this.tradState).some(ts => ts.visible);
      for (const ts of Object.values(this.tradState)) {
        ts.visible = !anyVisible;
        ts.kde     = !anyVisible;
        for (const cs of Object.values(ts.corpora)) {
          cs.visible = !anyVisible;
          cs.kde     = !anyVisible;
        }
      }
      this._recomputeActive();
      this.render('filter');
    },

    get anyTraditionVisible() {
      return Object.values(this.tradState).some(ts => ts.visible);
    },

    toggleTradition(tradName) {
      const ts = this.tradState[tradName];
      ts.visible = !ts.visible;
      // Cascade to corpora
      for (const cs of Object.values(ts.corpora)) cs.visible = ts.visible;
      this._recomputeActive();
      this.render('filter');
    },

    toggleTradKde(tradName) {
      this.tradState[tradName].kde = !this.tradState[tradName].kde;
      this.render('filter');
    },

    toggleCorpusKde(tradName, corpName) {
      this.tradState[tradName].corpora[corpName].kde = !this.tradState[tradName].corpora[corpName].kde;
      this.render('filter');
    },

    toggleCorpus(tradName, corpName) {
      const cs = this.tradState[tradName].corpora[corpName];
      cs.visible = !cs.visible;
      // Update tradition master — visible if any corpus visible
      const ts = this.tradState[tradName];
      ts.visible = Object.values(ts.corpora).some(c => c.visible);
      this._recomputeActive();
      this.render('filter');
    },

    toggleHeight(tradName, corpName, h) {
      const cs = this.tradState[tradName].corpora[corpName];
      cs.heights[h] = !cs.heights[h];
      this._recomputeActive();
      this.render('filter');
    },

    toggleTradExpand(tradName) {
      this.tradState[tradName].expanded = !this.tradState[tradName].expanded;
    },

    toggleCorpusExpand(tradName, corpName) {
      this.tradState[tradName].corpora[corpName].expanded =
        !this.tradState[tradName].corpora[corpName].expanded;
    },

    // Computed: is a tradition fully visible (all corpora on)?
    tradFullyVisible(tradName) {
      const ts = this.tradState[tradName];
      return ts?.visible && Object.values(ts.corpora).every(c => c.visible);
    },

    // Level name for a height in a corpus (falls back to "Level N")
    levelName(ci, h) {
      const levels = this.mapData?.corpus_levels?.[ci] || {};
      return levels[h] ?? `Level ${h}`;
    },

    // Sorted tradition names for rendering
    get tradNames() {
      return Object.keys(this.tradState).sort();
    },

    // -----------------------------------------------------------------------
    // deck.gl init
    // -----------------------------------------------------------------------
    _initDeck() {
      const canvas = document.getElementById('deck-canvas');
      const leaves = _rawPoints.filter(p => p.h === 0);

      let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity;
      for (const p of leaves) {
        if (p.x < xMin) xMin = p.x;
        if (p.x > xMax) xMax = p.x;
        if (p.y < yMin) yMin = p.y;
        if (p.y > yMax) yMax = p.y;
      }
      const xMid  = (xMin + xMax) / 2;
      const yMid  = (yMin + yMax) / 2;
      const span  = Math.max(xMax - xMin, yMax - yMin) * 1.05;
      const canvasW = canvas.clientWidth  || window.innerWidth;
      const canvasH = canvas.clientHeight || (window.innerHeight - 56);
      const zoom    = Math.log2(Math.min(canvasW, canvasH) / span);

      _deck = new deck.Deck({
        canvas,
        views: new deck.OrthographicView({ flipY: false }),
        initialViewState: {
          target:  [xMid, yMid, 0],
          zoom,
          minZoom: zoom - 4,
          maxZoom: zoom + 8,
        },
        controller: true,
        onViewStateChange: ({ viewState }) => {
          _zoom = viewState.zoom;
        },
        onHover:    info => this._onHover(info),
        onClick:    info => this._onClick(info),
        getTooltip: null,
      });

      _zoom        = zoom;
      _initialZoom = zoom;
    },

    // -----------------------------------------------------------------------
    // Render
    // -----------------------------------------------------------------------
    render(source = 'unknown') {
      if (!_deck || !_rawMapData) return;
      const t0   = performance.now();
      const data = _rawMapData;   // plain, never proxied
      const zoom = _zoom;

const layers = [];

      // -- KDE density clouds (per-tradition and per-corpus, filtered by tradState) --
      {
        const kdePolygons = [];
        for (const [tradName, ts] of Object.entries(this.tradState)) {
          if (ts.kde && _tradKde[tradName]) {
            for (const p of _tradKde[tradName]) kdePolygons.push(p);
          }
          for (const [corpName, cs] of Object.entries(ts.corpora)) {
            if (cs.kde && _corpKde[corpName]) {
              for (const p of _corpKde[corpName]) kdePolygons.push(p);
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
            getPolygon:     d => d.polygon,
            getFillColor:   d => [...hexToRgb(d.color), Math.round(d.alpha * 255)],
            getLineColor:   d => [...hexToRgb(d.color), 80],
            getLineWidth:   0.5,
            lineWidthUnits: 'pixels',
          }));
        }
      }

      // -- Active points scatter --
      if (this.layers.scatter && _deckActivePoints.length > 0) {
        layers.push(new deck.ScatterplotLayer({
          id: 'points',
          data: _deckActivePoints,
          pickable: true,
          opacity: 0.5,
          stroked: false,
          filled: true,
          radiusMinPixels: 1,
          radiusMaxPixels: 6,
          getRadius: d => d.h === 0 ? 0.08 : 0.18,
          getPosition: getPos3D,
          getFillColor: d => {
            const rgb = hexToRgb(tradColor(data.traditions[d.ti]));
            if (this.highlightIds.size > 0) {
              return this.highlightIds.has(d.id) ? [...rgb, 255] : [...rgb, 40];
            }
            // Higher heights are brighter/larger
            const alpha = d.h === 0 ? 180 : 230;
            return [...rgb, alpha];
          },
          updateTriggers: {
            getFillColor: [this.highlightIds.size, this.highlightIds],
            getRadius:    [],
          },
        }));
      }

      // -- Labels (tradition + corpus merged into one TextLayer to avoid atlas conflicts) --
      if (this.layers.labels || this.layers.corpusLabels) {
        // Build as plain Array (not Alpine proxy) — critical for deck.gl Proxy invariant
        const labelData = [];
        if (this.layers.corpusLabels) {
          for (const d of this._corpusCentroids(data))
            labelData.push({ ...d, size: 11, alpha: 175 });
        }
        if (this.layers.labels && zoom >= _initialZoom - 1) {
          for (const d of this._traditionCentroids(data))
            labelData.push({ ...d, size: 14, alpha: 210 });
        }
        if (labelData.length > 0) {
          layers.push(new deck.TextLayer({
            id: 'labels',
            data: labelData,
            pickable: false,
            getPosition:          getPos3D,
            getText:              d => d.name,
            getColor:             d => [...hexToRgb(d.color), d.alpha],
            getSize:              d => d.size,
            fontWeight:           700,
            background:           true,
            getBackgroundColor:   [15, 17, 23, 170],
            backgroundPadding:    [3, 2],
            fontFamily:           "'Segoe UI', system-ui, sans-serif",
            getTextAnchor:        'middle',
            getAlignmentBaseline: 'center',
            updateTriggers: {
              getColor: [this.layers.labels, this.layers.corpusLabels],
              getSize:  [this.layers.labels, this.layers.corpusLabels],
            },
          }));
        }
      }

      // -- Search constellation: lines from query verse to each result --
      if (this._queryPoint && this.results.length > 0) {
        const lineData = this.results
          .map(r => ({ src: this._queryPoint, tgt: _pointById.get(r.unit_id) }))
          .filter(d => d.tgt);

        layers.push(new deck.LineLayer({
          id: 'search-lines',
          data: lineData,
          pickable: false,
          getSourcePosition: d => [d.src.x, d.src.y],
          getTargetPosition: d => [d.tgt.x, d.tgt.y],
          getColor:  d => [...hexToRgb(tradColor(data.traditions[d.tgt.ti])), 200],
          getWidth:  1.5,
          widthUnits: 'pixels',
        }));

        layers.push(new deck.ScatterplotLayer({
          id: 'query-point',
          data: [this._queryPoint],
          pickable: false,
          getPosition:  getPos3D,
          getFillColor: [255, 255, 255, 220],
          getLineColor: [108, 140, 255, 255],
          stroked: true,
          filled:  true,
          getRadius:       0.15,
          radiusMinPixels: 5,
          radiusMaxPixels: 12,
          getLineWidth:    2,
          lineWidthUnits:  'pixels',
        }));
      }

      _deck.setProps({ layers });

      const dt = performance.now() - t0;
      this._renderCount++;
      this._lastRenderMs = dt;
      console.debug(
        `[render #${this._renderCount} ← ${source}] ${dt.toFixed(1)}ms | ` +
        `${layers.length} layers | ${this._activePoints.length} pts | zoom=${zoom.toFixed(2)}`
      );
    },

    // -----------------------------------------------------------------------
    // Hover / click
    // -----------------------------------------------------------------------
    _onHover(info) {
      const tooltip = document.getElementById('map-tooltip');
      if (!info.object) { tooltip.style.display = 'none'; return; }

      const d    = info.object;
      const data = _rawMapData;
      const trad = data.traditions[d.ti];
      const ttRef = document.getElementById('tt-ref');
      ttRef.textContent  = d.label || '';
      ttRef.style.color  = tradColor(trad);
      document.getElementById('tt-trad').textContent =
        `${trad} · ${this.shortName(data.corpora[d.ci])}`;
      document.getElementById('tt-text').textContent = '';

      tooltip.style.display = 'block';
      tooltip.style.left = (info.x + 12) + 'px';
      tooltip.style.top  = (info.y + 12) + 'px';

      if (d.h === 0) {
        fetch(`/api/v1/unit/${d.id}`)
          .then(r => r.ok ? r.json() : null)
          .then(v => {
            if (v && tooltip.style.display !== 'none')
              document.getElementById('tt-text').textContent = v.text;
          })
          .catch(() => {});
      }
    },

    _onClick(info) {
      if (!info.object) return;
      const d    = info.object;
      const data = _rawMapData;
      this.sidebarOpen = true;
      this.searchTab   = 'history';

      fetch(`/api/v1/unit/${d.id}`)
        .then(r => r.ok ? r.json() : null)
        .then(v => {
          if (!v) return;
          const entry = {
            unit_id:   d.id,
            label:     d.label,
            tradition: data.traditions[d.ti],
            corpus:    data.corpora[d.ci],
            text:      v.text,
          };
          // Deduplicate — most recent first
          this.history = [entry, ...this.history.filter(h => h.unit_id !== d.id)];
          this.activeResult = d.id;
        })
        .catch(() => {});
    },

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------
    async doVerseSearch() {
      if (!this.acValid) return;
      this.searchLoading = true;
      this.searched = true;
      try {
        const params = new URLSearchParams({
          corpus: this.selectedCorpus,
          ref:    this.refInput,
          limit:  50,
          offset: 0,
        });
        for (const c of this._visibleCorpora()) params.append('corpora', c);
        const rows = await fetch('/api/v1/verse?' + params).then(r => {
          if (!r.ok) throw new Error(r.statusText);
          return r.json();
        });
        const ci = _rawMapData.corpora.indexOf(this.selectedCorpus);
        this._queryPoint = _rawPoints.find(p => p.h === 0 && p.ci === ci && p.label === this.refInput) || null;
        this.results      = rows;
        this.highlightIds = new Set(rows.map(r => r.unit_id));
        this.render('verse-search');
        if (rows.length > 0) this.fitResults();
      } catch(e) {
        console.error(e);
      } finally {
        this.searchLoading = false;
      }
    },

    async doTextSearch() {
      const q = this.queryInput.trim();
      if (!q) return;
      this.searchLoading = true;
      this.searched = true;
      try {
        const params = new URLSearchParams({ q, limit: 50, offset: 0 });
        for (const c of this._visibleCorpora()) params.append('corpora', c);
        const rows = await fetch('/api/v1/search?' + params).then(r => {
          if (!r.ok) throw new Error(r.statusText);
          return r.json();
        });
        this._queryPoint  = null;
        this.results      = rows;
        this.highlightIds = new Set(rows.map(r => r.unit_id));
        this.render('text-search');
        if (rows.length > 0) this.fitResults();
      } catch(e) {
        console.error(e);
      } finally {
        this.searchLoading = false;
      }
    },

    async doSimilarSearch(unit_id) {
      this.searchLoading = true;
      this.searched      = true;
      this.searchTab     = 'verse';
      try {
        const params = new URLSearchParams({ limit: 50, offset: 0 });
        for (const c of this._visibleCorpora()) params.append('corpora', c);
        const rows = await fetch(`/api/v1/similar/${unit_id}?${params}`).then(r => {
          if (!r.ok) throw new Error(r.statusText);
          return r.json();
        });
        // Set query point to the clicked unit's UMAP position (any height)
        this._queryPoint = _rawPoints.find(p => p.id === unit_id) || null;
        this.results      = rows;
        this.highlightIds = new Set(rows.map(r => r.unit_id));
        this.render('similar-search');
        if (rows.length > 0) this.fitResults();
      } catch(e) {
        console.error(e);
      } finally {
        this.searchLoading = false;
      }
    },

    fitResults() {
      if (!_deck || !_rawMapData || this.results.length === 0) return;
      const idSet = new Set(this.results.map(r => r.unit_id));
      const pts   = _rawPoints.filter(p => idSet.has(p.id));
      if (!pts.length) return;

      let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity;
      for (const p of pts) {
        if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x;
        if (p.y < yMin) yMin = p.y; if (p.y > yMax) yMax = p.y;
      }
      const xMid  = (xMin + xMax) / 2;
      const yMid  = (yMin + yMax) / 2;
      const span  = Math.max(xMax - xMin, yMax - yMin) * 1.4 || 2;
      const minDim = Math.min(
        document.getElementById('deck-canvas').clientWidth,
        document.getElementById('deck-canvas').clientHeight
      ) || 600;

      _deck.setProps({
        initialViewState: {
          ..._deck.props.initialViewState,
          target: [xMid, yMid, 0],
          zoom:   Math.log2(minDim / span),
          transitionDuration: 600,
        },
      });
    },

    zoomToResult(r) {
      const pt = _pointById?.get(r.unit_id);
      if (!pt || !_deck) return;
      _deck.setProps({
        initialViewState: {
          ..._deck.props.initialViewState,
          target:             [pt.x, pt.y, 0],
          zoom:               _initialZoom + 5,
          transitionDuration: 500,
        },
      });
    },

    hoverResult(r) {
      this.activeResult = r.unit_id;
      const pt = _pointById?.get(r.unit_id);
      if (pt && _deck) {
        _deck.setProps({
          initialViewState: {
            ..._deck.props.initialViewState,
            target:             [pt.x, pt.y, 0],
            transitionDuration: 400,
          },
        });
      }
    },

    unhoverResult() { this.activeResult = null; },

    selectResult(r) {
      this.zoomToResult(r);
      this.activeResult = r.unit_id;
    },

    clearResults() {
      this.results      = [];
      this.highlightIds = new Set();
      this.searched     = false;
      this.activeResult = null;
      this._queryPoint  = null;
      this.render('clear');
    },

    // -----------------------------------------------------------------------
    // Autocomplete
    // -----------------------------------------------------------------------
    async _acFetch(q, limit) {
      if (!this.selectedCorpus) return;
      try {
        const params = new URLSearchParams({ corpus: this.selectedCorpus, q, limit });
        const items  = await fetch('/api/v1/refs?' + params).then(r => r.json());
        this.acItems  = items;
        this.acActive = -1;
        this.acOpen   = items.length > 0;
      } catch { this.acOpen = false; }
    },

    acFocus() {
      if (!this.refInput.trim()) this._acFetch('', 3);
    },

    acInput() {
      this.acValid = false;
      clearTimeout(this._acTimer);
      const val = this.refInput.trim();
      if (!val) { this._acFetch('', 3); return; }
      this._acTimer = setTimeout(() => this._acFetch(val, 20), 180);
    },

    acKeydown(e) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (this.acOpen) this.acActive = Math.min(this.acActive + 1, this.acItems.length - 1);
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (this.acOpen) this.acActive = Math.max(this.acActive - 1, -1);
      } else if (e.key === 'Enter') {
        if (this.acOpen && this.acActive >= 0) {
          e.preventDefault();
          this.acSelect(this.acItems[this.acActive]);
        } else {
          this.acOpen = false;
          this.doVerseSearch();
        }
      } else if (e.key === 'Escape') {
        this.acOpen = false;
      }
    },

    acSelect(item) {
      this.refInput = item;
      this.acValid  = true;
      this.acOpen   = false;
    },

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    shortName(name) {
      return name.replace(/\s*\([^)]+\)\s*$/, '').replace(/^Bible\s*[—-]\s*/, '');
    },

    // Returns list of corpus names currently visible in tradState
    _visibleCorpora() {
      if (!_rawMapData) return [];
      const visible = [];
      for (let ci = 0; ci < _rawMapData.corpora.length; ci++) {
        const corpName = _rawMapData.corpora[ci];
        const tradName = _rawMapData.traditions[_rawMapData.trad_of_corpus[ci]];
        const ts = this.tradState[tradName];
        if (!ts?.visible) continue;
        if (ts.corpora[corpName]?.visible) visible.push(corpName);
      }
      return visible;
    },

    _traditionCentroids(data) {
      const byTrad = {};
      for (const p of _deckActivePoints) {
        if (p.h !== 0) continue;
        const t = data.traditions[p.ti];
        if (!byTrad[t]) byTrad[t] = { xs: [], ys: [] };
        byTrad[t].xs.push(p.x);
        byTrad[t].ys.push(p.y);
      }
      return Object.entries(byTrad).map(([name, { xs, ys }]) => ({
        name,
        color: tradColor(name),
        x: xs.reduce((a, b) => a + b, 0) / xs.length,
        y: ys.reduce((a, b) => a + b, 0) / ys.length,
      }));
    },

    _corpusCentroids(data) {
      const byCorpus = {};
      for (const p of _deckActivePoints) {
        if (p.h !== 0) continue;
        const c = data.corpora[p.ci];
        if (!byCorpus[c]) byCorpus[c] = { xs: [], ys: [], ti: p.ti };
        byCorpus[c].xs.push(p.x);
        byCorpus[c].ys.push(p.y);
      }
      return Object.entries(byCorpus).map(([name, { xs, ys, ti }]) => ({
        name: this.shortName(name),
        color: tradColor(data.traditions[ti]),
        x: xs.reduce((a, b) => a + b, 0) / xs.length,
        y: ys.reduce((a, b) => a + b, 0) / ys.length,
      }));
    },

    _computeKDE() {
      // Group leaves by tradition and corpus
      const byTrad = {};
      const byCorpus = {};
      for (const p of _rawPoints) {
        if (p.h !== 0) continue;
        const tradName = _rawMapData.traditions[p.ti];
        const corpName = _rawMapData.corpora[p.ci];
        if (!byTrad[tradName]) byTrad[tradName] = [];
        if (!byCorpus[corpName]) byCorpus[corpName] = [];
        byTrad[tradName].push(p);
        byCorpus[corpName].push(p);
      }

      // Global bounds for consistent grid
      let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity;
      for (const p of _rawPoints) {
        if (p.h !== 0) continue;
        if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x;
        if (p.y < yMin) yMin = p.y; if (p.y > yMax) yMax = p.y;
      }

      const GRID   = 200;
      const xScale = d3.scaleLinear().domain([xMin, xMax]).range([0, GRID]);
      const yScale = d3.scaleLinear().domain([yMin, yMax]).range([0, GRID]);

      const _kdePolygons = (pts, color, bandwidth, thresholds) => {
        const density = d3.contourDensity()
          .x(d => xScale(d.x))
          .y(d => yScale(d.y))
          .size([GRID, GRID])
          .bandwidth(bandwidth)
          .thresholds(thresholds)(pts);
        if (!density.length) return [];
        const maxVal    = density[density.length - 1].value;
        const minThresh = maxVal * 0.08;
        const result = [];
        for (const contour of density) {
          if (contour.value < minThresh) continue;
          const alpha = 0.04 + 0.12 * (contour.value / maxVal);
          for (const ring of contour.coordinates) {
            result.push({
              polygon: ring.map(coords => coords.map(([gx, gy]) => [xScale.invert(gx), yScale.invert(gy)])),
              color,
              alpha,
            });
          }
        }
        return result;
      };

      // Per-tradition KDE (broader bandwidth, more contours)
      _tradKde = {};
      for (const [tradName, pts] of Object.entries(byTrad)) {
        _tradKde[tradName] = _kdePolygons(pts, tradColor(tradName), 8, 4);
      }

      // Per-corpus KDE (tighter bandwidth, fewer contours — corpora can be small)
      _corpKde = {};
      for (const [corpName, pts] of Object.entries(byCorpus)) {
        const ti = _rawMapData.trad_of_corpus[_rawMapData.corpora.indexOf(corpName)];
        const color = tradColor(_rawMapData.traditions[ti]);
        _corpKde[corpName] = _kdePolygons(pts, color, 5, 3);
      }
    },
  };
}
