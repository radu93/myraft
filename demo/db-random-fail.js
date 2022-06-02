var DummyDb = require('../lib/dummy-db')
var randomstring = require("randomstring")
const fs = require('fs')

const NUM_NODES = 100
const NUM_FAILURES = 49
const TEST_TIME = 20 * 1000 // 60 seconds

const WRITE_INTERVAL = 200 // ms
const MIN_KILL_INTERVAL = 1000 // ms
const MAX_KILL_INTERVAL = 2000 // ms

const MIN_ELECTION_INTERVAL = 300 // ms
const MAX_ELECTION_INTERVAL = 500 // ms
const HEARTBEAT_INTERVAL = 50 // ms

const DUMP_DIR = '/home/radu/master/distr_alg/myraft/demo/data/'
const DUMP_PREFIX = DUMP_DIR + 'node'

var nodes = []

// startup db nodes
for (i = 0; i < NUM_NODES; i++) {
    nodes.push(new DummyDb({
        heartbeatInterval: HEARTBEAT_INTERVAL,
        electionTimeout: {
            min: MIN_ELECTION_INTERVAL,
            max: MAX_ELECTION_INTERVAL
        },
        clusterSize: NUM_NODES,
        dump_path: DUMP_PREFIX + i
    }))
}

nodes[0]._raft.on('committed', function (data) {
    console.log(data)
})  

function randomInteger(min, max) {
    return Math.floor(Math.random() * (max - min)) + min;
}

// kill random nodes at random intervals
var killed = 0
var nodes_killed = []
var kill_timer

function toggleKill() {
    if (killed++ >= NUM_FAILURES) {
        console.log('Done killing', killed - 1, 'nodes')
        return
    }

    var idx = randomInteger(1, NUM_NODES)
    console.log(' -xx- Killing node', idx)

    if (nodes[idx] !== undefined) {
        nodes[idx].down()
        nodes[idx] = undefined
        nodes_killed.push('node' + idx)
    }

    kill_timer = setTimeout(toggleKill, randomInteger(MIN_KILL_INTERVAL, MAX_KILL_INTERVAL))
}

kill_timer = setTimeout(toggleKill, MIN_KILL_INTERVAL)

// write random values to random nodes repeatedly
var write_timer = setInterval(function() {

    // find alive node
    var idx = randomInteger(0, NUM_NODES)
    while (nodes[idx] == undefined) {
        idx = randomInteger(0, NUM_NODES)
    }

    console.log('Writing to node', nodes[idx]._id)
    nodes[idx].write(randomstring.generate(8), randomstring.generate(8))

}, WRITE_INTERVAL)

// stop and test consistency
setTimeout(function() {

    clearInterval(write_timer)
    clearTimeout(kill_timer)
    console.log('Test finished')

    setTimeout(function() {
        console.log('Nodes killed', nodes_killed)

        fs.readdir(DUMP_DIR, function(err, files) {
            if (err) {
                return console.log('Unable to scan directory: ' + err);
            }
            var prevBuf
            for (let i = 0; i < files.length; i++) {
                if (!nodes_killed.includes(files[i])) {
                    var currBuf = fs.readFileSync(DUMP_DIR + files[i]);
                    if (prevBuf !== undefined && !prevBuf.equals(currBuf)) {
                        console.log('FAILED: Found mismatch at', files[i])
                        process.exit(1)
                    }
                }
            }
            console.log('SUCCESS')
        });
    }, 2000)
}, TEST_TIME)
