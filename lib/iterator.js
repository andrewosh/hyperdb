var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var lock = require('mutexify')
var options = require('./options')
var hash = require('./hash')

module.exports = Iterator

function Iterator (db, prefix, opts) {
  if (!(this instanceof Iterator)) return new Iterator(db, prefix, opts)
  if (!opts) opts = {}

  nanoiterator.call(this)

  this._db = db
  this._recursive = opts.recursive !== false
  this._prefix = prefix
  this._end = 0
  this._gt = !!opts.gt
  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)

  this._stack = []
  this._done = false
  this._lock = lock()
}

inherits(Iterator, nanoiterator)

Iterator.prototype._open = function (cb) {
  var self = this

  // do a prefix search to find the heads for the prefix
  // we are trying to scan
  var opts = {prefix: true, map: false, reduce: false}
  this._db.get(this._prefix, opts, function (err, heads) {
    if (err) return cb(err)
    console.log('HEADS:', heads)

    var prefixLength = hash(self._prefix, false).length

    self._offset = prefixLength
    self._end = prefixLength + (self._recursive ? Infinity : hash.LENGTH)

    self._push(heads, self._offset)
    return cb()
  })
}

Iterator.prototype._push = function (nodes, i, allowConflict) {
  if (!nodes.length) {
    this._stack.push({ trie: null, i, nodes })
    return 
  }
  var trie = (nodes.length > 1) ? mergeTries(nodes) : nodes[0].trie
  nodes.sort(function (n1, n2) {
    return n1.path[i] < n2.path[i]
  })
  if (allowConflict) this._stack.push({ trie, i, nodes: nodes })
  else {
    for (var j = 0; j < nodes.length; j++) {
      console.log('PUSHING: node,', nodes[j], 'trie:', trie)
      this._stack.push({ trie, i, nodes: [nodes[j]] })
    }
  }
}

Iterator.prototype._loadNodes = function (bucket, cb) {
  var remaining = bucket.length
  var nodes = []
  var pending = false

  for (var i = 0; i < bucket.length; i++) {
    var next = bucket[i]
    if (!next) {
      remaining--
      continue
    }
    pending = true
    this._db._getPointer(next.feed, next.seq, false, done)
  }
  console.log('here and pending:', pending)
  if (!pending) process.nextTick(done, null, null)

  function done(err, node) {
    console.log('In loadNodes done')
    if (err) return cb(err)
    if (node) nodes.push(node)
    console.log('remaining:', remaining, 'bucket:', bucket)
    if (--remaining === 0) return cb(null, nodes)
  }
}

Iterator.prototype._loadNext = function (top, i, cb) {
  // Do a BFS search of the trie based data structure
  // Results are sorted based on the path hash.
  var self = this
  var bucket = top.trie[i]
  var nodes = top.nodes

  console.log('_loadNext, nodes:', nodes, 'bucket:', bucket, 'i:', i)

  var remaining = 5
  var nodesByPath = []

  for (var j = 0; j < nodes.length; j++) {
    var idx = sortValue(nodes[j].path[i])
    if (nodesByPath[idx]) nodesByPath[idx].push(nodes[j])
    else nodesByPath[idx] = [nodes[j]]
  }

  console.log('_loadNext initial nodesByPath:', nodesByPath, 'bucket:', bucket, 'i:', i)

  var gt = this._gt || !this._start
  var sortEnd = gt && this._start === i ? 4 : 5
  var pending = false

  this._lock(function (release) {
    for (let j = 0; j < sortEnd; j++) {
      if (!bucket[j]) { 
        remaining--
        continue
      }
      pending = true
      self._loadNodes(bucket[j], function (err, nodes) {
        if (err) return cb(err)
        var idx = sortValue(j)
        console.log('AFTER LOADING:', nodes, 'idx:', idx)
        if (nodesByPath[idx]) nodesByPath[idx].push.apply(nodesByPath[idx], nodes)
        else nodesByPath[idx] = nodes
        return done(release)
      })
    }
    console.log('at end, pending:', pending)
    if (!pending) return done(release)
  })

  function done (release) {
    console.log('LOAD, remaining:', remaining)
    if (--remaining <= 0) {
      // Insert the nodes in reverse order (so they'll be hash-ordered during iteration).
      console.log('PUSHING nodesByPath:', nodesByPath, 'self._stack:', self._stack)
      for (var j = nodesByPath.length - 1; j >= 0; j--) {
        var toPush = nodesByPath[j]
        if (!toPush) continue
        var allowConflict = toPush.length && sameKey(toPush)
        self._push(toPush, i + 1, allowConflict)
      }
      release()
      return cb(null, !!nodesByPath.length)
    }
  }

  function sortValue (i) {
    return !i ? 0 : i
  }
}

Iterator.prototype._next = function (cb) {
  var self = this

  console.log('STACK:', this._stack)

  var top = this._stack.pop()
  if (!top) return process.nextTick(cb, null, null)
  if (!top.trie || !top.trie.length) return this._filterResult(top.nodes, cb)

  console.log('TOP:', top.nodes)

  var pending = 0

  for (let i = top.i; i < this._end; i++) {
    var bucket = top.trie[i]
    console.log('I:', i, 'top.trie[i]:', top.trie[i])
    if (i > top.trie.length - 1) break
    if (!bucket || !bucket.length) continue

    pending = true

    console.log('NEXT, loading next')
    this._loadNext(top, i, function (err, pushed) {
      if (err) return cb(err)
      console.log('NEXT, After loadNext, pushed:', pushed)
      if (!pushed) return self._filterResult(top.nodes, cb)
      return self._next(cb)
    })
    break
  }
  if (!pending) return this._filterResult(top.nodes, cb)
}

// TODO: This should not be necessary if authorization nodes are handled better above.
Iterator.prototype._filterResult = function (nodes, cb) {
  console.log('FILTERING NODES:', nodes)
  if (!nodes) return process.nextTick(cb, null, null)
  if (!nodes.length || !nodes[0].key) return this._next(cb)
  nodes = nodes.filter(n => n.value)
  return process.nextTick(cb, null, this._prereturn(nodes))
}

Iterator.prototype._prereturn = function (nodes) {
  console.log('RETURNING NODES:', nodes)
  console.log('HASH:', hash(nodes[0].key).join(''))
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
}

function mergeTries (nodes) {
  var ptrSets = {}
  var tries = nodes.map(n => n.trie)
  var mergedTrie = new Array(nodes.reduce(maxTrieSize, 0))
  var headsAndPositions = getHeadsAndPositions(tries)
  headsAndPositions.heads.forEach(function (seq, feed) {
    var pos = headsAndPositions.pos.get(feed)
    var i = pos.i
    var j = pos.j

    if (!mergedTrie[i]) mergedTrie[i] = []
    if (!mergedTrie[i][j]) mergedTrie[i][j] = []
    mergedTrie[i][j] = { feed, seq } 
  })
  console.log('MERGED TRIE:', mergedTrie)
  return mergedTrie
}

function getHeadsAndPositions (tries) {
  return trieReduce(function (agg, i, j, bucket) {
    for (var i = 0; i < bucket.length; i++) {
      var feed = bucket[i].feed
      var seq = bucket[i].seq
      var head = agg.heads[feed]
      if (!head || (head && head < seq)) {
        agg.heads.set(feed, seq)
        agg.pos.set(feed, { i, j })
      }
    }
    return agg
  }, { heads: new Map(), pos: new Map()}, tries)
}

function trieReduce (func, initial, tries) {
  return tries.reduce(function (agg, trie) {
    if (!trie) return agg
    for (var i = 0; i < trie.length; i++) {
      if (!trie[i]) continue
      for (var j = 0; j < 5; j++) {
        var bucket = trie[i][j]
        if (bucket && bucket.length) {
          func(agg, i, j, bucket)
        }
      }
    }
    return agg
  }, initial)
}

function maxTrieSize (s, n) {
  return Math.max(s, n.trie.length)
}

function sameKey () {
  console.log('SAME KEY arguments:', arguments)
  var key = null
  var same = true
  for (var i = 0; i < arguments.length; i++) {
    var k = arguments[i].key
    if (key && k !== key) same = false
    key = k
  }
  console.log('same:', same)
  return same
}
