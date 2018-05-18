const path = require('./path')

const buckets = [0, 1, 2, 3, path.SEPARATE, path.TERMINATE]

/**
 * Algorithm:
 * 1. Starting with the HEAD values, check if any node matches criterion, if so return all nodes
 *    with that key.
 * 2. Else, search for the next candidate buckets given the path value at the current pointer,
 *    and recurse.
 * 3. If there are no more candidate buckets, return [].
 */
function orderedSearch (bucketSelector, sorter) {
  return function (db, key, path, ptr, nodes, cb) {
    console.log('path:', path, 'ptr:', ptr, 'nodes:', nodes)
    var pathVal = path[ptr]

    // If there are no more candidates to explore, then the value doesn't exist.
    if (ptr < 0) return cb(null, [])

    // If any nodes match the search criterion, return all nodes with the matching key.
    var matchingNodes = findMatchingNodes(key, nodes, sorter)
    if (matchingNodes.length) return cb(null, matchingNodes)

    var buckets = bucketSelector(pathVal)
    console.log('buckets:', buckets)
    console.log('pathVal:', pathVal)

    // Else find the next buckets to explore, based on the search criterion (lt or gt), and recurse.
    searchTries(db, nodes, pathVal, ptr, buckets, function (err, nextNodes) {
      if (err) return cb(err)
      console.log('nextNodes:', nextNodes)
      // Move towards the top of the trie.
      return _gt(db, key, path, --ptr, nextNodes, cb)
    })
  }
}

var _gt = orderedSearch(gtBuckets, compareKeys)
var _lt = orderedSearch(ltBuckets, function ltCompare (a, b) {
  return -1 * compareKeys(a, b)
})

module.exports.lt = function (db, key, cb) {
  var path = db._path(key, false)
  db.heads(function (err, heads) {
    if (err) return cb(err)
    return _lt(db, key, path, path.length - 1, heads, cb)
  })
}

module.exports.gt = function (db, key, cb) {
  var path = db._path(key, false)
  db.heads(function (err, heads) {
    if (err) return cb(err)
    return _gt(db, key, path, path.length - 1, heads, cb)
  })
}

function findMatchingNodes (key, nodes, sorter) {
  var matches = []
  var matchKey

  console.log('finding matches for key', key, 'nodes', nodes)

  var sorted = (nodes.length === 1) ? nodes : nodes.sort(function (a, b) {
    return sorter(a.key, b.key)
  })
  for (var i = 0; i < sorted.length; i++) {
    var node = sorted[i]
    if (!matchKey) {
      if (sorter(node.key, key) > 0) {
        matchKey = node.key  
        matches.push(node)
      }
    } else if (matchKey && node.key === matchKey) {
      matches.push(node)
    } else {
      break
    }
  }
  console.log('matches:', matches)
  return matches
}

function searchTries (db, nodes, pathVal, ptr, buckets, cb) {
  var candidates = []
  var pending = 0

  console.log('searching tries:', nodes, 'pathVal:', pathVal, 'ptr:', ptr, 'buckets:', buckets)

  nodes.forEach(function (node) {
    var outerBucket = node.trie[ptr]
    console.log('outerBucket:', outerBucket)
    if (!outerBucket) return

    for (var i = 0; i < buckets.length; i++) {
      var innerBucket = outerBucket[buckets[i]]
      console.log('innerBucket:', innerBucket)
      if (!innerBucket) continue

      pending++

      db._getAllPointers(innerBucket, false, function (err, nodes) {
        if (err) return cb(err)
        pending--
        handleCandidates(nodes)
      })
      break
    }
  })

  console.log('pending:', pending)
  if (!pending) return cb(null, [])

  function handleCandidates (nodes) {
    candidates = candidates.concat(nodes)
    if (!pending) return cb(null, candidates)
  }
}

function compareKeys (a, b) {
  return b.localeCompare(a)
}

function sameKeyFilter (l) {
  if (!l.length) return []
  var first = l[0]
  return l.filter(function (node) {
    return node.key === first.key
  })
}

function gtBuckets (pathVal) {
  return buckets.slice(pathVal, buckets.length)
}

function ltBuckets (pathVal) {
  return buckets.slice(0, pathVal).reverse()
}
