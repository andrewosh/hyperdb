var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var lock = require('mutexify')
var options = require('./options')
var hash = require('./hash')

module.exports = Iterator

const actions = {
  END: 1,
  CONTINUE: 2,
  AGGREGATE: 3
}

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
  if (nodes.length > 1 && !allowConflict) {
    // If we're pushing many nodes, and conflicts aren't allowed (i.e. they have
    // differing keys), then we must create a virtual node representing the fork.
    var entry = createMergedEntry(nodes)
    console.log('MERGED ENTRY:', JSON.stringify(entry))
    entry.i = i
    var entries = [entry]
  } else {
    var entries = nodes.map((n, j) => {
      return { trie: n.trie, i, nodes: [nodes[j]] }
    })
  }
  for (var j = 0; j < entries.length; j++) {
    console.log('PUSHING: entry,', entries[j])
    this._stack.push(entries[j])
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
  var fork = top.fork

  console.log('_loadNext, nodes:', nodes, 'bucket:', bucket, 'i:', i)

  var remaining = 5
  var nodesByPath = []

  if (!top.fork) {
    for (var j = 0; j < nodes.length; j++) {
      var idx = sortValue(nodes[j].path[i])
      if (nodesByPath[idx]) nodesByPath[idx].push(nodes[j])
      else nodesByPath[idx] = [nodes[j]]
    }
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
        var allowConflict = toPush.length && same(toPush.map(n => n.key))
        self._push(toPush, (fork) ? i : i + 1, allowConflict)
      }
      release()
      return cb(null, !!nodesByPath.length)
    }
  }

  function sortValue (i) {
    console.log('SORT VALUE:', i)
    return (!i ||  i === 4) ? 0 : i
  }
}

Iterator.prototype._next = function (cb) {
  var self = this

  console.log('STACK:', this._stack)

  var top = this._stack.pop()
  if (!top) return process.nextTick(cb, null, null)
  if (!top.trie || !top.trie.length) return done()

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
      if (!pushed) return done()
      return self._next(cb)
    })
    break
  }
  if (!pending) return done()

  function done () {
    var action = getAction(top.nodes)
    switch (action) {
      case actions.END:
        return cb(null, null)
      case actions.CONTINUE:
        return self._next(cb)
      case actions.AGGREGATE:
        return cb(null, self._prereturn(aggregate()))
    }
  }

  function aggregate () {
    var results = top.nodes
    for (var i = self._stack.length - 2; i >= 0; i--) {
      var next = self._stack[i]
      if (!next.nodes || !next.nodes.length ||
        !(next.nodes[0].key === top.nodes[0].key)) {
        return results
      }
      results = results.concat(next.nodes)
    }
    console.log('AGGREGATED:', results)
    return results
  }

  function getAction (nodes) {
    if (!nodes) return actions.END
    if (!nodes.length || !nodes[0].key) return actions.CONTINUE
    return actions.AGGREGATE
  }
}

Iterator.prototype._prereturn = function (nodes) {
  console.log('RETURNING NODES:', nodes)
  console.log('HASH:', hash(nodes[0].key).join(''))
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
}

/*
 * Forks are handled by creating a single "virtual node" that stores
 * path information for all conflicting nodes in its trie. These virtual
 * nodes are filtered out of the iteration results.
 */
function createMergedEntry (nodes) {
  // 1) First find the greatest common path prefix for the given nodes.
  // 2) Construct a prefix trie out of everything past the common prefix.
  if (!nodes || !nodes.length) return null

  var prefix = null
  var maxLength = nodes.reduce(maxPathLength, 0)
  var trie = new Array(maxLength)

  for (var i = 0; i < maxLength; i++) {
    if (same.apply(null, nodes.map(n => n.path[i]))) continue
    prefix = nodes[0].path.slice(0, i)
    break
  }

  for (i = 0; i < nodes.length; i++) {
    var node = nodes[i]
    for (var j = prefix.length; j < maxLength; j++) {
      var pathVal = node.path[j]
      var outerBucket = trie[j]

      if (node.path[j] !== prefix[j]) {
        if (!outerBucket) outerBucket = trie[j] = []
        var bucket = outerBucket[pathVal]
        if (!bucket) bucket = outerBucket[pathVal] = []
        bucket.push({ feed: node.feed, seq: node.seq })
        break
      }
    }
  }

  var virtualNode = { trie, nodes: [{ path: prefix }], fork: true }
  return virtualNode
}

function maxPathLength (s, n) {
  return Math.max(s, n.path.length)
}

function same () {
  var v = null
  var same = true
  for (var i = 0; i < arguments.length; i++) {
    var l = arguments[i]
    if (v != null && v !== l) same = false
    v = l
  }
  return same
}
