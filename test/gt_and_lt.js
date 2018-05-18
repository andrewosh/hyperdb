var tape = require('tape')
var create = require('./helpers/create')
var put = require('./helpers/put')
var run = require('./helpers/run')

function checkOrdering(t, db, orderFunc, sorter, vals, cb) {
  var sortedVals = vals.sort(sorter)  
  var prev = null
  console.log('sortedVals:', sortedVals)
  var expectedNexts = sortedVals.reduce(function (acc, next) {
    acc[next] = prev
    prev = next
    return acc
  }, {})
  console.log('expectedNexts:', expectedNexts)
  var ops = Object.keys(expectedNexts).map(function (key) {
    return function (cb) {
      orderFunc(key, function (err, nodes) {
        t.error(err, 'no error')
        if (!expectedNexts[key]) {
          t.false(nodes.length)
        } else {
          t.same(nodes[0].key, expectedNexts[key])
        }
        return cb()
      })
    }
  })
  run(ops, cb)
}

tape('simple gt', function (t) {
  var db = create.one(null, { lexint: true })
  var vals = ['0', '1', '9', 'a', 'aa', 'z']
  put(db, vals, function (err) {
    t.error(err, 'no error')
    checkOrdering(t, db, db.gt.bind(db), compareKeys, vals, function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape.skip('simple lt', function (t) {
  var db = create.one(null, { lexint: true })
  var vals = ['0', '1', '9', 'a', 'aa', 'z']
  var sorter = function (a, b) {
    return -1 * compareKeys(a, b)
  }
  put(db, vals, function (err) {
    t.error(err, 'no error')
    checkOrdering(t, db, db.lt.bind(db), sorter, vals, function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('gt with two-feed conflict')
tape('lt with two-feed conflict')

function compareKeys (a, b, reverse) {
  return b.localeCompare(a)
}
