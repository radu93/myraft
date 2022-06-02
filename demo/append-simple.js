var myraft = require('..')
var uuid = require('uuid')

var opts = {
  id: uuid.v4(),
  clusterSize: 3,
  heartbeatInterval: 1000,
  electionTimeout: {
    min: 3500,
    max: 5000
  }
}

opts.id = uuid.v4()
var nodeA = myraft(opts)

opts.id = uuid.v4()
var nodeB = myraft(opts)

opts.id = uuid.v4()
var nodeC = myraft(opts)

nodeA.on('committed', function (data) {
  console.log(data)
})

nodeA.append('first')