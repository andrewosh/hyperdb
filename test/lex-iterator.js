var tape = require('tape')
var create = require('./helpers/create')
var put = require('./helpers/put')

tape('lex iteration with no bounds, single db', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, false, db.lexIterator(), keys, function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iteration with no bounds, single db, reversed', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, true, db.lexIterator({ reverse: true }), keys, function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

function testIteratorOrder (t, reverse, iterator, expected, done) {
  var sorted = expected.slice().sort()
  if (reverse) sorted.reverse()
  each(iterator, onEach, onDone)
  function onEach (err, node) {
    t.error(err, 'no error')
    var key = node.key || node[0].key
    t.same(key, sorted.shift())
  }
  function onDone () {
    t.same(sorted.length, 0)
    if (done === undefined) t.end()
    else done()
  }
}

function each (ite, cb, done) {
  ite.next(function loop (err, node) {
    if (err) return cb(err)
    if (!node) return done()
    cb(null, node)
    ite.next(loop)
  })
}
