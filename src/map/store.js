// Plain JS data stores for deck.gl — kept outside Alpine's reactive proxy.
//
// deck.gl marks layer.data non-writable/non-configurable via Object.defineProperty,
// which fires the Proxy invariant when Alpine wraps these arrays. Any array or
// object passed as `data:` to a deck.gl layer must come from here, never from `this.*`.
export const store = {
  rawMapData: null,
  rawPoints: [],
  byCorpusHeight: {},  // ci -> h -> point[]
  pointById: null,     // Map<id, point>
  deckActivePoints: [], // current visible subset passed to ScatterplotLayer
  deckActiveIds: new Set(), // Set<id> of currently displayed points
  tradKde: {},         // tradName -> [{polygon, color, alpha}]
  corpKde: {},         // corpName -> [{polygon, color, alpha}]
  deck: null,          // Deck.gl instance (must live outside Alpine)
  zoom: 0,
  initialZoom: 0,
  hoverTimer: null,
}

// Stable accessor — same reference across render() calls so deck.gl skips GPU re-uploads
export const getPos3D = d => [d.x, d.y, 0]
