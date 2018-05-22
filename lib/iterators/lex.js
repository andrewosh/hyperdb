var nanoiterator = require('nanoiterator')
var inherits = require('inherits')

var path = require('../path')
var options = require('../options')

var SORT_ORDER = [path.TERMINATE, path.SEPARATE, 0, 1, 2, 3]

// This is used to avoid lots of `indexOf`s.
var SORT_BOUNDS = {}
SORT_BOUNDS[path.TERMINATE] = 0
SORT_BOUNDS[path.SEPARATE] = 1
SORT_BOUNDS[0] = 2
SORT_BOUNDS[1] = 3
SORT_BOUNDS[2] = 4
SORT_BOUNDS[3] = 5

module.exports = LexIterator

function LexIterator (db, opts) {
  if (!(this instanceof LexIterator)) return new LexIterator(db, opts)
  if (!opts) opts = {}
  this._db = db
  this._opts = opts

  nanoiterator.call(this)

  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)

  this._gt = this._opts.gt
  this._lt = this._opts.lt

  this._workers = new Map()
}
inherits(LexIterator, nanoiterator)

LexIterator.prototype._handleFork = function (heads, index) {
  var self = this

  var forks = computeForks(heads)
  console.log('forks:', forks)

  forks.forEach(function (fork, i) {
    // Don't reinsert an existing worker.
    console.log('ITERATING OVER HEAD FOR FEED:', heads[i].feed)
    if (self._workers.get(heads[i].feed)) return
    self._workers.set(heads[i].feed, new Worker(self, self._db, heads[i], fork, index, {
      reverse: self._opts.reverse,
      lower: self._lower,
      upper: self._upper
    }))
  })
}

LexIterator.prototype._open = function (cb) {
  var self = this

  this._db.heads(function (err, heads) {
    if (err) return cb(err)

    var opts = self._opts
    self._lower = self._db._path(opts.gte || opts.gt, false)
    self._upper = self._db._path(opts.lte || opts.lt, false)

    self._handleFork(heads, 0)

    return cb(null)
  })
}

LexIterator.prototype._next = function (cb) {
  var self = this

  if (!this._workers.size) return cb(null, null)

  var pending = this._workers.size
  var forked = false

  console.log('ITE searching workers:', self._workers.size)
  console.log('ITE stacks:')
  self._workers.forEach(worker => { console.log('  stack:', worker._stack) })
  this._workers.forEach(function (worker) {
    console.log('calling search')
    worker.search(function (err) {
      if (err && err.fork) forked = true
      else if (err) return cb(err)
      console.log('search completed, forked:', forked)
      if (!--pending) {
        return onFinish()
      }
    })
  })

  /**
   * TODO: consolidate iterations over the results list.
   */
  function onFinish () {
    console.log('ALL SEARCHES DONE')
    // If there was a fork during the search, retry.
    if (forked) return self._next(cb)

    var results = []
    self._workers.forEach(function (worker, feed) {
      results.push({
        value: worker.value,
        feed
      })
    })

    console.log('ITE search finished, results:', results)

    // Remove ended workers.
    for (var i = 0; i < results.length; i++) {
      if (results[i].ended) {
        self._workers.delete(results[i].feed)
      }
    }

    console.log('RESULTS:', results)

    // Filter nulls.
    results = results.filter(function (result) {
      return result.value
    })

    if (!results.length) return cb(null, null)

    // Ensure that the results are in the correct order.
    results.sort(function (a, b) {
      return byKey(a.value, b.value, self._opts.reverse)
    })

    // Group by key.
    console.log('ABOUT TO GROUP:', results)
    var result = []
    for (i = 0; i < results.length; i++) {
      if (results[i].value.key === results[0].value.key) {
        result.push(results[i].value)
        self._workers.get(results[i].feed).value = null
      } else {
        break
      }
    }

    // Do gte and lte boundary filtering.
    if (result[0].key === self._gt) return self._next(cb)
    if (result[0].key === self._lt) return self._next(cb)

    return cb(null, self._prereturn(result))
  }
}

LexIterator.prototype._prereturn = function (nodes) {
  if (this._map) nodes = nodes.map(this._map)
  if (this._reduce) return nodes.reduce(this._reduce)
  console.log('ITE, RETURNING:', nodes)
  return nodes
}

function Worker (iterator, db, head, fork, startIndex, opts) {
  this._head = head
  this._feed = head.feed
  this._iterator = iterator
  this._db = db

  // `fork` is the clock value of the point at which this worker
  // was forked from another worker. Iteration will not progress
  // past that point.
  this._fork = fork

  this._reverse = opts.reverse
  this._upper = opts.upper
  this._lower = opts.lower
  this._gte = !!opts.gte
  this._lte = !!opts.lte

  console.log('UPPER:', this._upper)
  console.log('LOWER:', this._lower)

  this._stack = [{
    i: startIndex,
    node: head
  }]

  this.value = null
}

/**
 * Searching a node's trie for candidate values proceeds as follows:
 * 1. First to an lt pass, finding all pointers that reference nodes which are less than
 *    the current node, and also bounded by lower and upper.
 * 2. Re-insert the current node into the stack, but without an index (it won't be re-explored).
 *    (note: the node will only be reinserted if it's within upper/lower)
 * 3. Do a gt pass (same logic as the lt pass).
 * 4. Optionally reverse the list of candidates.
 * 5. Push the new list of candidates into the stack.
 */
Worker.prototype._searchTrie = function (node, index, gtSafe, ltSafe) {
  console.log('ITE searching trie:', JSON.stringify(node.trie), 'path:', node.path, 'at index:', index)
  var ltCandidates = []
  var gtCandidates = []

  index = (index === 0) ? index : index + 1
  var nodeInBounds = true

  for (var i = index; i < node.path.length; i++) {
    var pathVal = node.path[i]
    var bucket = node.trie[i]
    var upper = this._upper && this._upper[i]
    var lower = this._lower && this._lower[i]

    console.log('ITE lt pathVal:', pathVal, 'i:', i, 'node.path:', node.path)
    for (var j = 0; j < SORT_ORDER.length; j++) {
      var idx = SORT_ORDER[j]

      var bounds
      // If this subtrie is safe, we don't have to do anymore bounds checking.
      if (!(gtSafe && ltSafe)) bounds = getBounds(idx, upper, lower)
      console.log('  ITE bounds:', bounds, 'bucket:', bucket, 'bucket[idx]:', bucket && bucket[idx])
      if (bucket && bucket[idx] &&
          (ltSafe || withinBounds(bounds[0])) &&
          (gtSafe || withinBounds(bounds[1]))) {
        console.log('  ITE  pushing candidate')
        var slot = (SORT_BOUNDS[idx] < SORT_BOUNDS[pathVal]) ? ltCandidates : gtCandidates
        slot.push({
          ptr: bucket[idx],
          i,
          ltSafe: ltSafe || isSafe(bounds[0]),
          gtSafe: gtSafe || isSafe(bounds[1])
        })
      }
    }

    var nodeBounds = getBounds(pathVal, upper, lower)
    ltSafe = ltSafe || isSafe(nodeBounds[0])
    gtSafe = gtSafe || isSafe(nodeBounds[1])
    // Stop the search if all subsequent pointers will be out of bounds.
    console.log('ITE gtSafe:', gtSafe, 'ltSafe:', ltSafe, 'nodeInBounds:', nodeInBounds,
                'nodeBounds:', nodeBounds, 'upper:', upper, 'lower:', lower)
    if ((!gtSafe && !withinBounds(nodeBounds[1])) ||
        (!ltSafe && !withinBounds(nodeBounds[0]))) {
      nodeInBounds = false
      console.log('ITE BREAKING')
      break
    }
  }

  console.log('  ITE AFTER SEARCH ltCandidates:', ltCandidates, 'gtCandidates:', gtCandidates,
    'gtSafe:', gtSafe, 'ltSafe:', ltSafe)

  var middleSlot = nodeInBounds ? [{ node }] : []
  var candidates = ltCandidates.concat(middleSlot).concat(gtCandidates)

  if (!this._reverse) candidates.reverse()
  this._stack = this._stack.concat(candidates)

  console.log('  ITE stack:', this._stack)

  function getBounds (val, upper, lower) {
    console.log('  ITE in outOfBounds, val:', val, 'upper:', upper, 'lower:', lower)
    var bounds = [
      upper !== undefined && SORT_BOUNDS[upper] - SORT_BOUNDS[val],
      lower !== undefined && SORT_BOUNDS[val] - SORT_BOUNDS[lower]
    ]
    console.log('    ITE bounds:', bounds)
    return bounds
  }

  function withinBounds (bounds) {
    if (typeof bounds !== 'object') return bounds === false || bounds >= 0
    return (bounds[0] === false || bounds[0] >= 0) && (bounds[1] === false || bounds[1] >= 0)
  }

  function isSafe (bounds) {
    if (typeof bounds !== 'object') return bounds === false || bounds > 0
    return (bounds[0] === false || bounds[0] > 0) && (bounds[1] === false || bounds[1] > 0)
  }
}

Worker.prototype._outsideRange = function (clock) {
  if (!this._fork) return false
  return smallerClock(clock, this._fork)
}

Worker.prototype.search = function (cb) {
  var self = this

  console.log('in search, this.value:', this.value)

  // If our last result was not consumed, offer it again.
  if (this.value) {
    return process.nextTick(cb, null)
  }
  // If there are no more candidates, and no more nodes to explore, then the worker is ended.
  if (this._stack.length === 0) {
    return process.nextTick(cb, null)
  }

  var top = this._stack.pop()

  console.log('ITE in search, top:', top)

  // If the new result does not require further trie exploration, and isn't a deletion, return it.
  if (top.i === undefined) {
    if (top.node.value === null) {
      return process.nextTick(this.search.bind(this), cb)
    }
    this.value = top.node
    console.log('ITE setting value to:', this.value)
    return process.nextTick(cb, null)
  }

  // If the result represents a fork:
  // 1) Create any new workers for feeds that aren't being tracked.
  // 2) Continue exploring this worker's feed by selecting its pointer from the fork list and
  //    exploring it as usual.
  // 3) Tell the iterator to retry the `next` operation (to make the new workers search).
  if (top.ptr && top.ptr.length > 1) {
    console.log('in search, its a fork')
    return self._db._getAllPointers(top.ptr, false, function (err, nodes) {
      if (err) return cb(err)
      var selfNode = nodes.filter(function (node) {
        return node.feed === self._feed
      })[0]

      console.log('this._head:', self._head, 'nodes:', nodes)
      self._iterator._handleFork(nodes, top.i)
      top.node = selfNode
      top.ptr = null

      console.log('REPUSHING top:', top)
      self._stack.push(top)

      err = new Error('Search found a fork')
      err.fork = true
      return cb(err)
    })
  }

  // At this point, there won't be any forks.
  if (top.ptr) top.ptr = top.ptr[0]
  if (!top.node) {
    console.log('in search, getting pointer:', top.ptr)
    this._db._getPointer(top.ptr.feed, top.ptr.seq, false, function (err, node) {
      if (err) return cb(err)
      top.node = node
      console.log('calling searchAndFinish')
      return searchAndFinish()
    })
  } else {
    return searchAndFinish()
  }

  function searchAndFinish () {
    console.log('in searchAndFinish')
    // If this worker has reached the end of its range, do not search for a new value.
    if (self._outsideRange(top.node.clock)) {
      console.log('OUTSIDE RANGE')
      return cb(null)
    }

    // Get all the candidate buckets for the given top node and retry the search.
    // (this will continue until the top node cannot be explored further)
    console.log('ITE calling searchTrie for node:', top.node, 'i:', top.i)
    self._searchTrie(top.node, top.i, top.gtSafe, top.ltSafe)
    return self.search(cb)
  }
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
  return forks
}

function leastCommonAncestor (c1, c2) {
  if (c1.length < c2.length) {
    var shorter = c1
    var longer = c2
  } else {
    shorter = c2
    longer = c1
  }
  return shorter.map((v, i) => Math.min(v, longer[i]))
}

function largerClock (c1, c2) {
  if (c2.length < c1.length) return false
  var larger = true
  for (var i = 0; i < c1.length; i++) {
    larger = larger && c1[i] >= c2[i]
  }
  return larger
}

function smallerClock (c1, c2) {
  if (c2.length > c1.length) return false
  var smaller = true
  for (var i = 0; i < c1.length; i++) {
    smaller = smaller && c1[i] <= c2[i]
  }
  return smaller
}

function byKey (a, b, reverse) {
  var k = b.key.localeCompare(a.key)
  return (reverse ? -1 : 1) * (k || b.feed - a.feed)
}

