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

  // Set in _open.
  this._offset = null
  this._workers = []
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

Iterator.prototype._createWorker = function (head, offset, fork) {
  var w = new Worker(this, this._db, head, offset,  this._end, fork)
  this._workers.push(w)
}

Iterator.prototype._next = function (cb) {
  var self = this
}

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

function Worker (iterator, db, head, start, end, fork) {
  this._iterator = iterator
  this._db = db
  this._start = start
  this._end = end

  // `fork` is the clock value of the point at which this worker
  // was forked from another worker. Iteration will not progress
  // past that point.
  this._fork = fork

  this.ended = false
  this.stack = [{ nodes: [head], i: this._start }]
}

Worker.prototype.maybeFork = function () {
  if (!this.stack.length) return
  var next = this.stack.pop()
  if (next.nodes.length <= 1) {
    this.stack.push(next)
    return
  }
  // This is a fork. Compute the minimum clock value that each fork
  // should stop at, then create new workers for each clock.
  var forks = this._computeForks(next.nodes)
  for (var i = 0; i < forks.length; i++) {
    this._iterator.createWorker(next.nodes[i], next.i, forks[i])
  }
  return
}

Worker.prototype.search = function (cb) {
  if (!stack.length) {
    this.ended = true
    return process.nextTick(cb, null, null)
  }

  var next = this.stack.pop()
  // Since all forking nodes are processed in maybeFork, `next` is
  // guaranteed to only have single-value nodes at this point.
  var node = next.nodes[0] 

  if (node && this._outsideRange(node.clock)) {
    // If this worker has reached its terminating clock value, discard
    // the current node and keep searching.
    return this.search(cb)
  }
  if (next.i < this._end) {
    var newEntries = this._findNewEntries(next, done)
  } else {
    // If we've reached the termination point, just return the current node.
    return process.nextTick(cb, null, next.nodes)
  }

  function done (err, newEntries) {
    if (err) return cb(err)

    // Nodes are pushed onto the stack such that they're hash sorted.
    if (newEntries.gt.length) {
      this.stack.push.apply(this.stack, newEntries.gt)
    }
    this.stack.push({ nodes: next.nodes, i: next.i + 1 })
    if (newEntries.lt.length) {
      this.stack.push.apply(this.stack, newEntries.lt)
    }
    // If none of the new nodes are smaller than the current one,
    // return the current node.
    if (!newEntries.lt.length) return cb(null, next.nodes)
  }
}

Worker.prototype._findNewNodes = function (entry, cb) {
  var self = this

  var node = entry.nodes[0]
  var pathVal = node.path[i]
  var bucket = node.trie[entry.i] 
  var newEntries = {
    lt: [],
    gt: []
  }

  if (!bucket) return {}

  var remaining = bucket.length
  for (let i = 0; i < remaining; i++) {
    var innerBucket = bucket[i]
    if (!innerBucket) {
      remaining--
      continue
    }
    self._db._getAllPointers(innerBucket, function (err, nodes) {
      if (err) return cb(err)
      insert(i, nodes)
    })
  }

  function insert (idx, nodes) {
    if (pathVal === 4 || idx < pathVal) {
      sortedBucket = newEntries.lt
    } else {
      sortedBucket = newEntries.gt
    }
    sortedBucket[idx] = nodes
    if (--remaining === 0) {
      // Some indices will not be encountered -- remove those entries.
      newEntries.lt = newEntries.lt.filter(v => v !== undefined)
      newEntries.gt = newEntries.gt.filter(v => v !== undefined)
      return cb(null, newEntries)
    }
  }
}

Worker.prototype._outsideRange = function (clock) {
  return smallerClock(clock, this._fork)
}

function smallerClock (c1, c2) {
  
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
