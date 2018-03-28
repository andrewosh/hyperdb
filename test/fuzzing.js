var tape = require('tape')

var run = require('./helpers/run')
var fuzzRunner = require('./helpers/fuzzing').fuzzRunner

tape('fuzz testing', function (t) {
  run(
    cb => fuzzRunner(t, {
      keys: 20,
      dirs: 2,
      dirSize: 2,
      conflicts: 0,
      replications: 2
    }, cb),
    cb => fuzzRunner(t, {
      keys: 7000,
      dirs: 20,
      dirSize: 20,
      conflicts: 100,
      writers: 4,
      replications: 0
    }, cb),
    function (err) {
      t.error(err)
      t.end()
    }
  )
})
