var tape = require('tape')
var create = require('./helpers/create')
var put = require('./helpers/put')

tape.skip('lex iterate with no bounds, single db', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  console.log('keys:', keys)
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, false, db.lexIterator(), keys, function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape.skip('lex iterate with no bounds, single db, reversed', function (t) {
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

tape.skip('lex iterate a big db', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = range(1000, '')
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, false, db.lexIterator(), keys, function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iterate a big db, reverse', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = range(1000, '')
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, true, db.lexIterator({ reverse: true }), keys, function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iterate with gt', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  console.log('keys:', keys)
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, false, db.lexIterator({ gt: 'a' }), ['aa', 'aab', 'b'], function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iterate with gt, reverse', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  console.log('keys:', keys)
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, true, db.lexIterator({ gt: 'a', reverse: true }), ['aa', 'aab', 'b'], function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iterate with lt', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  console.log('keys:', keys)
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, false, db.lexIterator({ lt: 'a' }), ['0aa', '0b', '1b'], function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iterate with lt, reverse', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  console.log('keys:', keys)
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, true, db.lexIterator({ lt: 'a', reverse: true }), ['0aa', '0b', '1b'], function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

function testIteratorOrder (t, reverse, iterator, expected, done) {
  var sorted = expected.slice().sort()
  if (reverse) sorted.reverse()
  console.log('sorted:', sorted)
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

function range (n, v) {
  // #0, #1, #2, ...
  return new Array(n).join('.').split('.').map((a, i) => v + i)
}

