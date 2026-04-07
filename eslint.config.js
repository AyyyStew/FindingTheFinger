import js from '@eslint/js'

export default [
  js.configs.recommended,
  {
    languageOptions: {
      globals: {
        window: true,
        document: true,
        fetch: true,
        console: true,
        performance: true,
        requestAnimationFrame: true,
        clearTimeout: true,
        setTimeout: true,
        // deck.gl and D3 are loaded via CDN as window globals
        deck: 'readonly',
        d3: 'readonly',
      },
    },
    rules: {
      'no-unused-vars': ['warn', { argsIgnorePattern: '^_' }],
    },
  },
]
