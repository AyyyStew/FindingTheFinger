// Shared tradition/corpus configuration used by both search and map pages

export const TRADITION_CONFIG = {
  Abrahamic:   { emoji: '📜', color: '#f5c518' },
  Buddhist:    { emoji: '☸',  color: '#fb923c' },
  Confucian:   { emoji: '📖', color: '#34d399' },
  Dharmic:     { emoji: '🕉', color: '#c084fc' },
  Norse:       { emoji: '🔨', color: '#93c5fd' },
  Shinto:      { emoji: '⛩', color: '#f9a8d4' },
  Sikh:        { emoji: '☬',  color: '#22d3ee' },
  Taoist:      { emoji: '☯',  color: '#d4d4d8' },
  Zoroastrian: { emoji: '🔥', color: '#f87171' },
}

export const CORPUS_EMOJI_MAP = {
  'Bible — KJV (King James Version)': '✝️',
  'Quran (Clear Quran Translation)':  '☪️',
}

export function hexToRgb(hex) {
  const r = parseInt(hex.slice(1, 3), 16)
  const g = parseInt(hex.slice(3, 5), 16)
  const b = parseInt(hex.slice(5, 7), 16)
  return [r, g, b]
}

export function hexToRgba(hex, alpha) {
  const [r, g, b] = hexToRgb(hex)
  return `rgba(${r},${g},${b},${alpha})`
}

export function tradColor(name) {
  return TRADITION_CONFIG[name]?.color || '#6c8cff'
}

export function tradEmoji(name) {
  return TRADITION_CONFIG[name]?.emoji || '📖'
}

export function corpusEmoji(corpus, tradition) {
  return CORPUS_EMOJI_MAP[corpus] || tradEmoji(tradition)
}

export function shortName(name) {
  return name.replace(/\s*\([^)]+\)\s*$/, '').replace(/^Bible\s*[—-]\s*/, '')
}

export function tradBadgeStyle(tradition) {
  const c = tradColor(tradition)
  if (!c.startsWith('#')) return ''
  return `color:${c};background:${hexToRgba(c, 0.15)};border-color:${hexToRgba(c, 0.3)}`
}

export function corpusBadgeStyle(corpus, tradition) {
  const c = tradColor(tradition)
  if (!c.startsWith('#')) return ''
  return `color:${c};background:${hexToRgba(c, 0.08)};border-color:${hexToRgba(c, 0.2)}`
}
