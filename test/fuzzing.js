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
      keys: 10,
      dirs: 1,
      dirSize: 2,
      conflicts: 0,
      writers: 2,
      replications: 1
    }, cb),
    function (err) {
      if (err) console.error('Fuzz testing errored:', err)
      else console.log('Fuzz testing completed with no error!')
    }
  )
})
