var tape = require('tape')
var create = require('./helpers/create')
var put = require('./helpers/put')
var run = require('./helpers/run')

tape('lex iterate with no bounds, single db', function (t) {
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

tape('two writers, simple fork, lt and gt bounds', function (t) {
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

      all(db1.lexIterator({ gt: '1', lt: '2' }), ondb1all)
      all(db2.lexIterator({ gt: '1', lt: '2' }), ondb2all)
    }

    function ondb2all (err, map) {
      t.error(err, 'no error')
      t.same(map, {'10': ['10']})
    }

    function ondb1all (err, map) {
      t.error(err, 'no error')
      t.same(map, {'10': ['10'], '1/0': ['1/0']})
    }
  })
})

tape('lex iterate two writers, one fork', function (t) {
  create.two({ lex: true }, function (db1, db2, replicate) {
    run(
      cb => db1.put('0', '0', cb),
      cb => db2.put('2', '2', cb),
      cb => db2.put('3', '3', cb),
      cb => db2.put('4', '4', cb),
      cb => db2.put('5', '5', cb),
      cb => db2.put('6', '6', cb),
      cb => db2.put('7', '7', cb),
      cb => db2.put('8', '8', cb),
      cb => db2.put('9', '9', cb),
      cb => replicate(cb),
      cb => db1.put('1', '1a', cb),
      cb => db2.put('1', '1b', cb),
      cb => replicate(cb),
      cb => db1.put('0', '00', cb),
      cb => replicate(cb),
      cb => db2.put('hi', 'ho', cb),
      done
    )

    function done (err) {
      t.error(err, 'no error')
      all(db1.lexIterator(), function (err, vals) {
        t.error(err, 'no error')
        t.same(vals, {
          '0': ['00'],
          '1': ['1a', '1b'],
          '2': ['2'],
          '3': ['3'],
          '4': ['4'],
          '5': ['5'],
          '6': ['6'],
          '7': ['7'],
          '8': ['8'],
          '9': ['9']
        })

        all(db2.lexIterator(), function (err, vals) {
          t.error(err, 'no error')
          t.same(vals, {
            '0': ['00'],
            '1': ['1a', '1b'],
            '2': ['2'],
            '3': ['3'],
            '4': ['4'],
            '5': ['5'],
            '6': ['6'],
            '7': ['7'],
            '8': ['8'],
            '9': ['9'],
            'hi': ['ho']
          })
          t.end()
        })
      })
    }
  })
})

tape('lex iterate two writers, one fork, many values', function (t) {
  var r = range(100, 'i')

  create.two({ lex: true }, function (db1, db2, replicate) {
    run(
      cb => db1.put('0', '0', cb),
      cb => db2.put('2', '2', cb),
      cb => db2.put('3', '3', cb),
      cb => db2.put('4', '4', cb),
      cb => db2.put('5', '5', cb),
      cb => db2.put('6', '6', cb),
      cb => db2.put('7', '7', cb),
      cb => db2.put('8', '8', cb),
      cb => db2.put('9', '9', cb),
      cb => replicate(cb),
      cb => db1.put('1', '1a', cb),
      cb => db2.put('1', '1b', cb),
      cb => replicate(cb),
      cb => db1.put('0', '00', cb),
      r.map(i => cb => db1.put(i, i, cb)),
      cb => replicate(cb),
      done
    )

    function done (err) {
      t.error(err, 'no error')

      var expected = {
        '0': ['00'],
        '1': ['1a', '1b'],
        '2': ['2'],
        '3': ['3'],
        '4': ['4'],
        '5': ['5'],
        '6': ['6'],
        '7': ['7'],
        '8': ['8'],
        '9': ['9']
      }

      r.forEach(function (v) {
        expected[v] = [v]
      })

      all(db1.lexIterator(), function (err, vals) {
        t.error(err, 'no error')
        t.same(vals, expected)
        all(db2.lexIterator(), function (err, vals) {
          t.error(err, 'no error')
          t.same(vals, expected)
          t.end()
        })
      })
    }
  })
})

tape('lex iterate two writers, fork', function (t) {
  t.plan(2 * 2 + 1)

  create.two({ lex: true }, function (a, b, replicate) {
    run(
      cb => a.put('a', 'a', cb),
      replicate,
      cb => b.put('a', 'b', cb),
      cb => a.put('b', 'c', cb),
      replicate,
      done
    )

    function done (err) {
      t.error(err, 'no error')

      all(a.lexIterator(), onall)
      all(b.lexIterator(), onall)

      function onall (err, map) {
        t.error(err, 'no error')
        t.same(map, {b: ['c'], a: ['b']})
      }
    }
  })
})

tape('lex iterate three writers, two forks', function (t) {
  t.plan(2 * 3 + 1)

  var replicate = require('./helpers/replicate')

  create.three({ lex: true }, function (a, b, c, replicateAll) {
    run(
      cb => a.put('a', 'a', cb),
      replicateAll,
      cb => b.put('a', 'ab', cb),
      cb => a.put('some', 'some', cb),
      cb => replicate(a, c, cb),
      cb => c.put('c', 'c', cb),
      replicateAll,
      done
    )

    function done (err) {
      t.error(err, 'no error')
      all(a.lexIterator(), onall)
      all(b.lexIterator(), onall)
      all(c.lexIterator(), onall)

      function onall (err, map) {
        t.error(err, 'no error')
        t.same(map, {a: ['ab'], c: ['c'], some: ['some']})
      }
    }
  })
})

tape('lex iterate three writers, two forks, and gt/lt bounds', function (t) {
  t.plan(2 * 3 + 1)

  var replicate = require('./helpers/replicate')

  create.three({ lex: true }, function (a, b, c, replicateAll) {
    run(
      cb => a.put('a', 'a', cb),
      replicateAll,
      cb => b.put('a', 'ab', cb),
      cb => a.put('some', 'some', cb),
      cb => replicate(a, c, cb),
      cb => c.put('c', 'c', cb),
      replicateAll,
      done
    )

    function done (err) {
      t.error(err, 'no error')
      all(a.lexIterator({ gt: 'a', lte: 'c' }), onall)
      all(b.lexIterator({ gt: 'a', lte: 'c' }), onall)
      all(c.lexIterator({ gt: 'a', lte: 'c' }), onall)

      function onall (err, map) {
        t.error(err, 'no error')
        t.same(map, {c: ['c']})
      }
    }
  })
})

tape('lex iterate three writers, two forks, and gt/lt bounds, and reverse', function (t) {
  var replicate = require('./helpers/replicate')
  var keys = ['some', 'c']

  create.three({ lex: true }, function (a, b, c, replicateAll) {
    run(
      cb => a.put('a', 'a', cb),
      replicateAll,
      cb => b.put('a', 'ab', cb),
      cb => a.put('some', 'some', cb),
      cb => replicate(a, c, cb),
      cb => c.put('c', 'c', cb),
      replicateAll,
      done
    )

    function done (err) {
      t.error(err, 'no error')
      var pending = 3
      testIteratorOrder(t, true, a.lexIterator({ gt: 'a', lte: 'sp', reverse: true }), keys, ondone)
      testIteratorOrder(t, true, b.lexIterator({ gt: 'a', lte: 'sp', reverse: true }), keys, ondone)
      testIteratorOrder(t, true, c.lexIterator({ gt: 'a', lte: 'sp', reverse: true }), keys, ondone)

      function ondone (err) {
        t.error(err, 'no error')
        if (!--pending) t.end()
      }
    }
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

function range (n, v) {
  // #0, #1, #2, ...
  return new Array(n).join('.').split('.').map((a, i) => v + i)
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
