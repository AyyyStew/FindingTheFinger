// Autocomplete methods for the search page Alpine component.
// Each function uses `this` — they are assigned as component methods and
// called by Alpine with `this` bound to the component proxy.

export async function _acFetch(q, limit) {
  if (!this.selectedCorpus) return
  try {
    const params = new URLSearchParams({ corpus: this.selectedCorpus, q, limit })
    const tree = await fetch('/api/v1/refs?' + params).then(r => r.json())
    const flat = []
    const walk = (nodes, depth) => {
      for (const n of nodes) {
        flat.push({ ...n, depth, children: undefined })
        if (n.children?.length) walk(n.children, depth + 1)
      }
    }
    walk(tree, 0)
    this.acItems = flat
    this.acActive = -1
    this.acOpen = flat.length > 0
  } catch {
    this.acOpen = false
  }
}

export function acFocus() {
  if (!this.refInput.trim()) this._acFetch('', 5)
}

export function acInput() {
  this.acValid = false
  this.acSelectedNode = null
  this.targetHeight = null
  clearTimeout(this._acTimer)
  const val = this.refInput.trim()
  if (!val) { this._acFetch('', 5); return }
  this._acTimer = setTimeout(() => this._acFetch(val, 30), 180)
}

export function acKeydown(e) {
  if (e.key === 'ArrowDown') {
    e.preventDefault()
    if (this.acOpen) this.acActive = Math.min(this.acActive + 1, this.acItems.length - 1)
  } else if (e.key === 'ArrowUp') {
    e.preventDefault()
    if (this.acOpen) this.acActive = Math.max(this.acActive - 1, -1)
  } else if (e.key === 'Enter') {
    if (this.acOpen && this.acActive >= 0) {
      e.preventDefault()
      this.acSelect(this.acItems[this.acActive])
    } else {
      this.acOpen = false
      this.doVerseSearch()
    }
  } else if (e.key === 'Escape') {
    this.acOpen = false
  }
}

export function acSelect(node) {
  this.refInput = node.label
  this.acSelectedNode = node
  this.targetHeight = node.height
  this.acValid = true
  this.acOpen = false
  this.sourceVerse = null
  fetch(`/api/v1/unit/${node.id}`)
    .then(r => r.ok ? r.json() : null)
    .then(d => { if (d) this.sourceVerse = d })
    .catch(() => {})
}
