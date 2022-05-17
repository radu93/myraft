var RPCInterface = require("./rpc-interface");
var util = require("util");
var _ = require("lodash");
var instanceMap = {};

function MemoryRPC() {
  RPCInterface.apply(this, Array.prototype.slice.call(arguments));
}

util.inherits(MemoryRPC, RPCInterface);

MemoryRPC.prototype._connect = function _connect() {
  var self = this;
  setTimeout(function () {
    instanceMap[self.id] = self;
    self._connected();
  }, 0);
};

MemoryRPC.prototype._disconnect = function _disconnect() {
  var self = this;
  setTimeout(function () {
    instanceMap[self.id] = null;
    self._disconnected();
  }, 0);
};

MemoryRPC.prototype._broadcast = function _broadcast(data) {
  var self = this;
  _.each(instanceMap, function (instance, key) {
    if (instance != null) {
      self._send(key, data);
    }
  });
};

MemoryRPC.prototype._send = function _send(nodeId, data) {
  var self = this;
  setTimeout(function () {
    if (instanceMap[nodeId] != null) {
      instanceMap[nodeId]._recieved(self.id, data);
    }
  }, 0);
};

module.exports = MemoryRPC;
