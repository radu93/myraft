var DummyDb = require('../lib/dummy-db')

var nodes = []

for (i = 0; i < 5; i++) {
    nodes.push(new DummyDb({
        clusterSize: 5,
        dump_path: '/home/radu/master/distr_alg/myraft/demo/data/node' + i
    }))
}

nodes[0].write('a', 'b')
nodes[1].write('c', 'd')

nodes[0]._raft.on("committed", function (data) {
    console.log(data);
});
