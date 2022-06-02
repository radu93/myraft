var myraft = require('..')
var uuid = require('uuid')

var opts = {
  id: uuid.v4(),
  clusterSize: 3,
  heartbeatInterval: 300,
  electionTimeout: {
    min: 1000,
    max: 2000
  }
}

var nodes = []
var values = [ 'this', 'is', 'some', 'phrase', 'which', 'will', 'get', 'distributed']

for (i = 0; i < 5; i++) {
    opts.id = uuid.v4()
    nodes.push(myraft(opts))
}

nodes[0].on('committed', function (data) {
  console.log(data)
})

for (i = 0; i < values.length; i++) {
    nodes[i % nodes.length].append(values[i])
}