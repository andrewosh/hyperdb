var nanoiterator = require('nanoiterator')
var inherits = require('inherits')
var path = require('../path')
var options = require('../options')

function LexIterator(db, opts) {
  if (!(this instanceof LexIterator)) return new LexIterator(db, opts)
  if (!opts) opts = {}
  this._db = db
  this._opts = opts

  nanoiterator.call(this)

  this._map = options.map(opts, db)
  this._reduce = options.reduce(opts, db)

  // Set in open.
  this._heads = null
  this._workers = []

  this._pending = 0
  this._error = null
}
inherits(LexIterator, nanoiterator)

LexIterator.prototype._open = function (cb) {
  var self = this

  this._db.heads(function (err, heads) {
    if (err) return cb(err)
    self._heads = heads

    var forks = computeForks(heads)
    var opts = self._opts
    var lower = self._db.path(opts.gte || opts.gt, false)
    var upper = self._db.path(opts.lte || opts.lt, false)

    self._workers = forks.map(function (fork, i) {
      return new Worker(self, self._db, heads[i], fork, {
        reverse: opts.reverse,
        lower,
        upper
      })
    })

    return cb(null)
  })
}

LexIterator.prototype._next = function (cb) {

}

function Worker (iterator, db, head, fork, opts) {
  this._head = head
  this._iterator = iterator
  this._db = db

  // `fork` is the clock value of the point at which this worker
  // was forked from another worker. Iteration will not progress
  // past that point.
  this._fork = fork

  this._reverse = opts.reverse
  this._upper = opts.upper
  this._lower = opts.lower

  this._stack = [{
    i: 0,
    node: head
  }]

  this.result = null
  this.ended = false
}

/**
 * Searching a node's trie for candidate values proceeds as follows
 * 1. First to an lt pass, finding all pointers that reference nodes which are less than
 *    the current node, and also bounded by lower and upper.
 * 2. Re-insert the current node into the stack, but without an index (it won't be re-explored).
 *    (note: the node will only be reinserted if it's within upper/lower)
 * 3. Do a gt pass (same logic as the lt pass).
 * 4. Optionally reverse the list of candidates.
 * 5. Push the new list of candidates into the stack.
 *
 * The lt and gt passes are mostly duplicated to minimize function call overhead, since this is
 * a hot path.
 */
Worker.prototype._searchTrie = function (node, index) {
  var candidates = []
  var oob = false

  // lt pass
  for (var i = 0; i < node.trie.length; i++) {
    var pathVal = node.path[i]
    var bucket = node.trie[i]
    var upper = this._upper && this._upper[i]
    var lower = this._lower && this._lower[i]
    for (var j = pathVal - 1; j >= 0; j--) {
      if (bucket && bucket[j] && !outOfBounds(j, upper, lower)) {
        candidates.push({
          ptr: bucket[j],
          i
        })
      }
    }
    // Stop the search if all subsequent pointers will be out of bounds.
    if (outOfBounds(pathVal, upper, lower)) {
      oob = true
      break
    }
  }

  if (!oob) {
    candidates.push({ node })
  }

  // gt pass
  for (var i = 0; i < node.trie.length; i++) {
    var pathVal = node.path[i]
    var bucket = node.trie[i]
    var upper = this._upper && this._upper[i]
    var lower = this._lower && this._lower[i]
    for (var j = pathVal + 1; j <= path.TERMINATE; j++) {
      if (bucket && bucket[j] && !outOfBounds(j, upper, lower)) {
        candidates.push({
          ptr: bucket[j],
          i
        })
      }
    }
    // Stop the search if all subsequent pointers will be out of bounds.
    if (outOfBounds(pathVal, upper, lower)) {
      break
    }
  }

  function outOfBounds (val, upper, lower) {
    return (((upper !== undefined) && val > upper)
      || ((lower != undefined) && val < lower))
  }
}

Worker.prototype.search = function (cb) {
  var self = this

  // If our last result was not consumed, offer it again.
  if (this.result) {
    return process.nextTick(cb, null, this.result)
  }
  var top = this.stack.pop()

  // If the new result does not require further trie exploration, return it.
  if (!top.i) {
    this.result = top.node
    return process.nextTick(cb, null, this.result)
  }

  if (!top.node) {
    this._db._getPointer(top.ptr.feed, top.ptr.seq, false, function (err, node) {
      if (err) return cb(err)
      top.node = node
      return searchAndFinish()
    })
  }

  function searchAndFinish () {
    // Get all the candidate buckets for the given top node.
    this._searchTrie(top.node, top.i)

    // Retry the search (this will continue until the top node cannot be explored further).
    return this.search(cb)
  }
}

function getPaths (nodes) {
  return nodes.map(function (node) {
    return node.path
  })
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
    var shorter = c2
    var longer = c1
  }
  return shorter.map((v, i) => Math.min(v, longer[i]))
}
