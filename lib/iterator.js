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
      console.log('forks:', forks)
      for (var i = 0; i < heads.length; i++) {
        var worker = self._createWorker(heads[i], self._offset, forks[i])
        self._workers.push(worker)
      }
    } else {
      var worker = self._createWorker(heads[0], self._offset, null)
      self._workers.push(worker)
    }
    return cb()
  })
}

Iterator.prototype._createWorker = function (head, offset, fork) {
  return new Worker(this, this._db, head, offset, this._end, fork)
}

Iterator.prototype._mapWorkers = function (func, cb) {
  each(this._workers, (worker, next) => {
    console.log('WORKER WITH HEAD:', worker._head, 'ENDED?:', worker.ended)
    if (worker.ended) return next()
    return func(worker, next)
  }, (err, results) => {
    console.log('AT END OF EACH, results:', results, 'err:', err)
    if (!cb) {
      if (err) throw err
    } else {
      if (err) return cb(err)
      return cb(null, results)
    }
  })
}

Iterator.prototype._processForks = function (cb) {
  var self = this

  this._mapWorkers((w, next) => {
    return next(null, w.maybeFork())
  }, function (err, newWorkers) {
    for (var i = 0; i < newWorkers.length; i++) {
      if (!newWorkers[i]) continue
      self._workers.push.apply(self._workers, newWorkers[i])
    }
    return cb()
  })
}

Iterator.prototype._search = function (cb) {
  this._mapWorkers((w, next) => {
    console.log('SEARCHING', w._head)
    w.search(err => {
      console.log('SEARCH DONE FOR HEAD:', w._head, 'err:', err)
      if (err) return next(err)
      return next(null)
    })
  }, cb)
}

Iterator.prototype._consume = function (cb) {
  var self = this

  this._mapWorkers((w, next) => {
    if (w.value) return next(null, [w.value, self._workers.indexOf(w)])
    else return next(null)
  }, (err, nodes) => {
    console.log('CONSUMING NODES:', nodes)
    nodes = nodes.filter(n => n !== undefined)
    console.log('ABOUT TO RETURN NODES:', nodes)
    if (!nodes.length) return cb(null, null)
    console.log('SORTING CANDIDATES:', nodes)
    sortCandidates(nodes)
    var results = select(nodes)
    return cb(null, self._prereturn(results))
  })

  function sortCandidates (nodes) {
    nodes.sort((ni1, ni2) => {
      var h1 = ni1[0].path.join('')
      var h2 = ni2[0].path.join('')
      return h1.localeCompare(h2)
    })
    console.log('SORTED NODES:', nodes)
  }

  function select (nodes) {
    console.log('SELECTING FROM:', nodes)
    var minKey = null
    var stopIdx = 0
    for (var i = 0; i < nodes.length; i++) {
      if (!minKey) minKey = nodes[i][0].key
      if (nodes[i][0].key === minKey) {
        stopIdx++
        continue
      }
      break
    }
    console.log('STOPIDX:', stopIdx, 'minKey:', minKey)
    for (i = 0; i < stopIdx; i++) {
      self._workers[nodes[i][1]].value = null
    }
    return nodes.slice(0, stopIdx).map(n => n[0])
  }
}

Iterator.prototype._next = function (cb) {
  var self = this

  // First compute any necessary forks.
  this._processForks(err => {
    console.log('PROCESSED FORKS, err:', err)
    if (err) return cb(err)
    // Then load each proposed value.
    self._search(err => {
      console.log('ALL SEARCHES ARE DONE')
      if (err) return cb(err)
      // Then select the minimum proposed values and update the workers
      // that proposed the selection.
      self._consume(cb)
    })
  })
}

Iterator.prototype._prereturn = function (nodes) {
  console.log('RETURNING NODES:', nodes)
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  return nodes
}

function Worker (iterator, db, head, start, end, fork) {
  this._head = head
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
  this.stack = [{ nodes: head ? [head] : [] , i: this._start }]
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
  var forks = computeForks(next.nodes)
  var newWorkers = new Array(forks.length)
  for (var i = 0; i < forks.length; i++) {
    newWorkers[i] = this._iterator._createWorker(next.nodes[i], next.i, forks[i])
  }
  return newWorkers
}

Worker.prototype.search = function (cb) {
  var self = this

  if (!this.stack.length) {
    this.ended = true
    console.log('STACK IS EMPTY')
    return process.nextTick(cb, null, null)
  } else if (this.value) {
    // If the previous value hasn't been consumed, return it now.
    console.log('REUSING OLD VALUE:', self.value)
    return process.nextTick(cb, null, null)
  }

  var next = this.stack.pop()
  console.log('HEAD:', this._head, 'NEXT:', next,  'STACK:', this.stack)

  if (!next.nodes.length) return process.nextTick(cb, null, null)

  // Since all forking nodes are processed in maybeFork, `next` is
  // guaranteed to only have single-value nodes at this point.
  var node = next.nodes[0]
  var trie = node.trie
  console.log('TRIE:', trie, 'trie.length:', trie.length, 'next.i:', next.i)

  if (node && this._outsideRange(node.clock)) {
    // If this worker has reached its terminating clock value, discard
    // the current node and keep searching.
    console.log('OUTSIDE clock:', node.clock, 'this._fork:', this._fork)
    return this.search(cb)
  }
  if (next.i < this._end && next.i < trie.length) {
    console.log('BEFORE END')
    return this._findNewEntries(next, done)
  } else {
    // If we've reached the termination point, just return the current node.
    console.log('AT TERMINATION')
    if (!next.nodes[0].key) return self.search(cb)
    this.value = next.nodes[0]
    return process.nextTick(cb, null)
  }

  function done (err, newEntries) {
    console.log('IN DONE, newEntries:', newEntries)
    if (err) return cb(err)

    // Nodes are pushed onto the stack such that they're hash sorted.
    if (newEntries.gt.length) {
      insert(newEntries.gt)
    }

    self.stack.push({ nodes: next.nodes, i: next.i + 1 })

    if (newEntries.lt.length) {
      insert(newEntries.lt)
    }

    return self.search(cb)
  }

  function insert (nodes) {
    self.stack.push.apply(self.stack, nodes.map(n => {
      return { nodes: n, i: next.i + 1 } 
    }))
  }
}

Worker.prototype._findNewEntries = function (entry, cb) {
  var self = this

  var node = entry.nodes[0]
  var pathVal = node.path[entry.i]
  var bucket = node.trie[entry.i] 
  var newEntries = {
    lt: [],
    gt: []
  }

  if (!bucket) return process.nextTick(cb, null, newEntries)

  var remaining = bucket.length
  for (let i = 0; i < bucket.length; i++) {
    var innerBucket = bucket[i]
    console.log('innerBucket:', innerBucket, 'bucket:', bucket)
    if (!innerBucket) {
      remaining--
      continue
    }
    console.log('GETTING POINTERS')
    self._db._getAllPointers(innerBucket, false, function (err, nodes) {
      console.log('AFTER GET, err:', err)
      if (err) return cb(err)
      console.log('INSERTING:', i, nodes)
      insert(i, nodes)
    })
  }

  function insert (idx, nodes) {
    console.log('IN INSERT, idx:', idx, 'NODES:', nodes)
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
  console.log('CHECKING OUTSIDE:', clock, 'this._fork:', this._fork)
  if (!this._fork) return false
  return smallerClock(clock, this._fork)
}

/*
 In order to prevent traversing duplicate regions of the trie, each
 fork worker must be assigned a clock value to stop at.
 The steps to compute this are as follows:

 1) For each head, do a pairwise calculation across all other heads
    to find its least common ancestors.
 2) Assign that LCA as the stopping point for the given node, and remove
    the node from the list.

 This requires 2n + 1 operations (n = # heads)
*/
function computeForks (nodes) {
  // TODO: Don't copy nodes into separate candidates array?
  var candidates = nodes.slice()
  var forks = []
  for (var i = 0; i < nodes.length; i++) {
    var minFork = null
    var c1 = nodes[i].clock
    for (var j = 0; j < candidates.length; j++) {
      var c2 = candidates[j].clock
      if (c1 === c2) {
        minFork = new Array(c1.length).fill(0)
      } else {
        var lca = leastCommonAncestor(c1, c2)
        if (!minFork || largerClock(minFork, lca)) {
          minFork = lca
        }
      }
    }
    forks.push(minFork)
    candidates.splice(candidates.indexOf(candidates[j], 1))
  }
  console.log('FORKS FOR NODES:', nodes.map(n => n.clock), 'FORKS:', forks)
  return forks
}

function leastCommonAncestor (c1, c2) {
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
  var larger = true  
  for (var i = 0; i < c1.length; i++) {
    larger = larger && c1[i] > c2[i]
  }
  return larger
}

function smallerClock (c1, c2) {
  console.log('OUTSIDE c1:', c1, 'c2:', c2)
  if (c2.length > c1.length) return false
  var smaller = true  
  for (var i = 0; i < c1.length; i++) {
    smaller = smaller && c1[i] <= c2[i]
  }
  console.log('OUTSIDE SMALLER:', smaller)
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
