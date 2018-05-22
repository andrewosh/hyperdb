var tape = require('tape')
var create = require('./helpers/create')
var put = require('./helpers/put')
var run = require('./helpers/run')

tape('lex iterate with no bounds, single db', function (t) {
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

tape('lex iterate with no bounds, single db, reversed', function (t) {
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

tape('lex iterate a big db', function (t) {
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

tape('lex iterate with both lt and gt', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  console.log('keys:', keys)
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, false, db.lexIterator({ lt: 'aa', gt: '0aa' }), ['a', '0b', '1b'], function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iterate with both lt and gt, reverse', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a', 'aab', 'aa', 'b', '0aa', '0b', '1b']
  console.log('keys:', keys)
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, true, db.lexIterator({ lt: 'aa', gt: '0aa', reverse: true }), ['a', '0b', '1b'], function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iterate a small part of a big db', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = range(10000, '')
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, false, db.lexIterator({ gt: '5555', lt: '5558' }), ['5556', '5557'], function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('lex iterate with paths', function (t) {
  var db = create.one(null, { lex: true, reduce: false })
  var keys = ['a/a', 'a/b', 'b/0', 'b/a', 'c/0', 'c/1', 'd/a', 'd/b']
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, false, db.lexIterator({ gt: 'b/1', lt: 'c/1' }), ['b/a', 'c/0'], function (err) {
      t.error(err, 'no error')
      t.end()
    })
  })
})

tape('two writers, simple fork, no bounds', function (t) {
  t.plan(2 * 2 + 1)

  create.two({ lex: true }, function (db1, db2, replicate) {
    run(
      cb => db1.put('0', '0', cb),
      replicate,
      cb => db1.put('1', '1a', cb),
      cb => db2.put('1', '1b', cb),
      cb => db1.put('10', '10', cb),
      replicate,
      cb => db1.put('2', '2', cb),
      cb => db1.put('1/0', '1/0', cb),
      done
    )

    function done (err) {
      t.error(err, 'no error')

      console.log('here')
      all(db1.lexIterator(), ondb1all)
      all(db2.lexIterator(), ondb2all)
    }

    function ondb2all (err, map) {
      t.error(err, 'no error')
      t.same(map, {'0': ['0'], '1': ['1a', '1b'], '10': ['10']})
    }

    function ondb1all (err, map) {
      t.error(err, 'no error')
      t.same(map, {'0': ['0'], '1': ['1a', '1b'], '10': ['10'], '2': ['2'], '1/0': ['1/0']})
    }
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

function toMap (list) {
  var map = {}
  for (var i = 0; i < list.length; i++) {
    map[list[i]] = list[i]
  }
  return map
}

function all (ite, cb) {
  var vals = {}

  ite.next(function loop (err, node) {
    if (err) return cb(err)
    if (!node) return cb(null, vals)
    var key = Array.isArray(node) ? node[0].key : node.key
    if (vals[key]) return cb(new Error('duplicate node for ' + key))
    vals[key] = Array.isArray(node) ? node.map(n => n.value).sort() : node.value
    ite.next(loop)
  })
}

