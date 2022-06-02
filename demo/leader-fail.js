var myraft = require('..')
var uuid = require('uuid')

var opts = {
  id: uuid.v4(),
  clusterSize: 5,
  heartbeatInterval: 1000,
  electionTimeout: {
    min: 3500,
    max: 5000
  }
}

var nodes = []

for (i = 0; i < 5; i++) {
    opts.id = uuid.v4()
    nodes.push(myraft(opts))
}

nodes[0].once('leaderElected', function() {
    for (i = 0; i < 5; i++) {
        if (nodes[i].isLeader()) {
            console.log(' -xx- killing leader')
            nodes[i].close()
        }
    }
})
