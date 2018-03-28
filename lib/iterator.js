var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var each = require('async-each')

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

    if (heads.length > 1) {
      var forks = computeForks(heads)
      for (var i = 0; i < heads.length; i++) {
        this._createWorker(heads[i], self._offset, forks[i])
      }
    } else {
      this._createWorker(heads[0], self._offset, null)
    }
    return cb()
  })
}

Iterator.prototype._createWorker = function (head, offset, fork) {
  var w = new Worker(this, this._db, head, offset,  this._end, fork)
  this._workers.push(w)
}

Iterator.prototype._mapWorkers = function (func, cb) {
  each(this._workers, (worker, next) => {
    if (worker.ended) process.nextTick(next)
    return func(worker, next)
  }, (err, results) => {
    if (!cb) {
      if (err) throw err
    } else {
      if (err) return cb(err)
      return cb(null, results)
    }
  })
}

Iterator.prototype._next = function (cb) {
  // First compute any necessary forks.
  this._mapWorkers((w, next) => {
    w.maybeFork()
    return next()
  })

  // Then load each proposed value.
  this._mapWorkers((w, next) => {
    w.search(err => {
      if (err) return next(err)
      return next()
    })
  })

  // Find and merge the minimum proposed values.
  this._mapWorkers((w, next) => {
    return next(null, [w.value, self._workers.indexOf(w)])
  }, (err, nodes) => {
    sortCandidates(nodes)
    var results = select(nodes)
    return cb(null, results)
  })

  function sortCandidates (nodes) {
    nodes.sort((ni1, ni2) => {
      var h1 = ni1[0].path.join('')
      var h2 = ni2[0].path.join('')
      return h1.localeCompare(h2)
    })
  }

  function select (nodes) {
    var minKey = null
    var stopIdx = 0
    for (var i = 0; i < nodes.length; i++) {
      if (!minKey) minKey = nodes[i].key
      if (nodes[i].key === minKey) continue
      stopIdx = i
      break
    }
    for (i = 0; i < stopIdx; i++) {
      // Consume the value from each accepted worker.
      self._workers[nodes[i][1]].value = null
    }
    return nodes.slice(0, stopIdx).map(n => n[0])
  }
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
  this.value = null
  this.stack = [{ nodes: [head], i: this._start }]
}

Worker.prototype.maybeFork = function () {
  if (!this.stack.length) return
  var next = this.stack.pop()
  if (!next.nodes || next.nodes.length <= 1) {
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
  var self = this

  if (!this.stack.length) {
    this.ended = true
    return process.nextTick(cb, null, null)
  } if (this.value) {
    // If the previous value hasn't been consumed, return.
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
    this.value = next.nodes
    return process.nextTick(cb, null)
  }

  function done (err, newEntries) {
    if (err) return cb(err)

    // Nodes are pushed onto the stack such that they're hash sorted.
    if (newEntries.gt.length) {
      self.stack.push.apply(self.stack, newEntries.gt)
    }
    self.stack.push({ nodes: next.nodes, i: next.i + 1 })
    if (newEntries.lt.length) {
      self.stack.push.apply(self.stack, newEntries.lt)
    }
    // If none of the new nodes are smaller than the current one,
    // return the current node.
    if (!newEntries.lt.length) {
      self.value = next.nodes
      return cb(null)
    }
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
    // Ensure that both the lt and gt buckets are correctly sorted.
    // If pathVal is the hash terminator, then all values should be lt.
    // (i.e. /a is less than /a/a)
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

function computeForks = function (nodes) {
  // In order to prevent traversing duplicate regions of the trie, each
  // fork worker must be assigned a clock value to stop at.
  // The steps to compute this are as follows:
  //
  // 1) For each head, do a pairwise calculation across all other heads
  //    to find its least common ancestors.
  // 2) Assign that LCA as the stopping point for the given node, and remove
  //    the node from the list.
  //
  // This requires 2n + 1 operations (n = # heads)
  // TODO: Don't copy nodes into separate candidates array?
  var candidates = nodes.slice()
  var forks = []
  for (var i = 0; i < nodes.length; i++) {
    var minFork = null
    var c1 = nodes[i].clock
    for (var j = 0; j < candidates.length; j++) {
      var c2 = candidates[j].clock
      var lca = leastCommonAncestor(c1, c2)
      if (!minFork || largerClock(minFork, lca)) {
        minFork = lca
      }
    }
    forks.push(minFork)
    candidates.splice(candidates.indexOf(candidates[j], 1))
  }
}

function leastCommonAncester (c1, c2) {
  if (c1.length < c2.length) {
    var shorter = c1 
    var longer = c2
  } else {
    var shorter = c2
    var longer = c1
  }
  return shorter.map((v, i) => Math.min(v, longer[i]))
}

function largerClock (c1, c2) {
  if (c2.length < c1.length) return false
  var smaller = true  
  for (var i = 0; i < c1.length; i++) {
    larger = larger && c1[i] > c2[i]
  }
  return larger
}

function smallerClock (c1, c2) {
  if (c2.length > c1.length) return false
  var smaller = true  
  for (var i = 0; i < c1.length; i++) {
    smaller = smaller && c1[i] < c2[i]
  }
  return smaller
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
