var _ = require("lodash");
var rpc = require("../rpc");
var MyRaft = require("./myraft");

module.exports = function MyRaftFactory(opts) {
  var rpcInst = new rpc["memory"]({
    id: opts.id,
    channelOptions: _.omit(opts.channel, "name"),
  });
  return new MyRaft(_.assign({}, opts, { channel: rpcInst }));
};

module.exports._STATES = MyRaft._STATES;
module.exports._RPC_TYPE = MyRaft._RPC_TYPE;
