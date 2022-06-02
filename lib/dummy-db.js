var raft = require('..')
var uuid = require('uuid')
const fs = require('fs')

function DummyDb(opts) {
    var self = this

    var raft_opts = {
        id: uuid.v4(),
        clusterSize: opts.clusterSize,
        heartbeatInterval: opts.heartbeatInterval || 300,
        electionTimeout: opts.electionTimeout || { min: 1000, max: 2000 }
    }

    this._raft = raft(raft_opts) // raft module
    this._db = {} // key value store
    this._dump_path = opts.dump_path
    this._id = raft_opts.id

    this._raft.on('committed', function(data) {
        var obj = JSON.parse(data.data)

        // update key value store
        self._db[obj['key']] = obj['value']

        // write to disk
        try {
            fs.writeFileSync(self._dump_path, JSON.stringify(self._db))
        } catch (err) {
            console.error(err)
        }
    })
}

DummyDb.prototype.write = function write(key, value) {
    this._raft.append(JSON.stringify({
        'key': key,
        'value': value
    }))
}

DummyDb.prototype.down = function down(cb) {
    this._raft.close(cb)
}

module.exports = DummyDb