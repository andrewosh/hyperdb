module.exports = function normalizeKey (key) {
  if (typeof key !== 'string') return key
  if (!key.length) return ''
  return key[0] === '/' ? key.slice(1) : key
}
