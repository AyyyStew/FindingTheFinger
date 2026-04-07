// deck.gl initialization, hover/click handlers, and camera helpers.
// deck.gl is loaded as a CDN global (window.deck) — not bundled.
import { store } from './store.js'
import { tradColor } from '../shared/constants.js'

export function _initDeck() {
  const canvas = document.getElementById('deck-canvas')
  const leaves = store.rawPoints.filter(p => p.h === 0)

  let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity
  for (const p of leaves) {
    if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x
    if (p.y < yMin) yMin = p.y; if (p.y > yMax) yMax = p.y
  }
  const xMid = (xMin + xMax) / 2
  const yMid = (yMin + yMax) / 2
  const span = Math.max(xMax - xMin, yMax - yMin) * 1.05
  const canvasW = canvas.clientWidth || window.innerWidth
  const canvasH = canvas.clientHeight || (window.innerHeight - 56)
  const zoom = Math.log2(Math.min(canvasW, canvasH) / span)

  store.deck = new deck.Deck({
    canvas,
    views: new deck.OrthographicView({ flipY: false }),
    initialViewState: {
      target: [xMid, yMid, 0],
      zoom,
      minZoom: zoom - 4,
      maxZoom: zoom + 8,
    },
    controller: true,
    onViewStateChange: ({ viewState }) => { store.zoom = viewState.zoom },
    onHover: info => this._onHover(info),
    onClick: info => this._onClick(info),
    getTooltip: null,
  })

  store.zoom = zoom
  store.initialZoom = zoom
}

export function _onHover(info) {
  const tooltip = document.getElementById('map-tooltip')
  clearTimeout(store.hoverTimer)

  if (!info.object) { tooltip.style.display = 'none'; return }

  const d = info.object
  const data = store.rawMapData
  const trad = data.traditions[d.ti]
  const ttRef = document.getElementById('tt-ref')
  ttRef.textContent = d.label || ''
  ttRef.style.color = tradColor(trad)
  document.getElementById('tt-trad').textContent =
    `${trad} · ${this.shortName(data.corpora[d.ci])}`
  document.getElementById('tt-text').textContent = ''

  tooltip.style.display = 'block'
  tooltip.style.left = (info.x + 12) + 'px'
  tooltip.style.top = (info.y + 12) + 'px'

  if (d.h === 0) {
    store.hoverTimer = setTimeout(() => {
      fetch(`/api/v1/unit/${d.id}`)
        .then(r => r.ok ? r.json() : null)
        .then(v => {
          if (v && tooltip.style.display !== 'none')
            document.getElementById('tt-text').textContent = v.text
        })
        .catch(() => {})
    }, 200)
  }
}

export function _onClick(info) {
  if (!info.object) return
  const d = info.object
  const data = store.rawMapData
  this.sidebarOpen = true
  this.searchTab = 'history'

  fetch(`/api/v1/unit/${d.id}`)
    .then(r => r.ok ? r.json() : null)
    .then(v => {
      if (!v) return
      const entry = {
        unit_id: d.id,
        label: d.label,
        height: d.h,
        tradition: data.traditions[d.ti],
        corpus: data.corpora[d.ci],
        text: v.text,
      }
      this.history = [entry, ...this.history.filter(h => h.unit_id !== d.id)]
      this.activeResult = d.id
    })
    .catch(() => {})
}

export function fitResults() {
  if (!store.deck || !store.rawMapData || this.results.length === 0) return
  const idSet = new Set(this.results.map(r => r.unit_id))
  const pts = store.rawPoints.filter(p => idSet.has(p.id))
  if (!pts.length) return

  let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity
  for (const p of pts) {
    if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x
    if (p.y < yMin) yMin = p.y; if (p.y > yMax) yMax = p.y
  }
  const xMid = (xMin + xMax) / 2
  const yMid = (yMin + yMax) / 2
  const span = Math.max(xMax - xMin, yMax - yMin) * 1.4 || 2
  const minDim = Math.min(
    document.getElementById('deck-canvas').clientWidth,
    document.getElementById('deck-canvas').clientHeight
  ) || 600

  store.deck.setProps({
    initialViewState: {
      ...store.deck.props.initialViewState,
      target: [xMid, yMid, 0],
      zoom: Math.log2(minDim / span),
      transitionDuration: 600,
    },
  })
}

export function zoomToResult(r) {
  const pt = store.pointById?.get(r.unit_id)
  if (!pt || !store.deck) return
  store.deck.setProps({
    initialViewState: {
      ...store.deck.props.initialViewState,
      target: [pt.x, pt.y, 0],
      zoom: store.initialZoom + 5,
      transitionDuration: 500,
    },
  })
}

export function hoverResult(r) {
  this.activeResult = r.unit_id
  const pt = store.pointById?.get(r.unit_id)
  if (pt && store.deck) {
    store.deck.setProps({
      initialViewState: {
        ...store.deck.props.initialViewState,
        target: [pt.x, pt.y, 0],
        transitionDuration: 400,
      },
    })
  }
}

export function unhoverResult() {
  this.activeResult = null
}

export function selectResult(r) {
  this.zoomToResult(r)
  this.activeResult = r.unit_id
}

export function clearResults() {
  this.results = []
  this.highlightIds = new Set()
  this.searched = false
  this.activeResult = null
  this._queryPoint = null
  this.render('clear')
}
