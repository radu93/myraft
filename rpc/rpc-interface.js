var EventEmitter = require("events").EventEmitter;
var util = require("util");
var _ = require("lodash");

function RPCInterface(opts) {
  EventEmitter.call(this);

  this.id = opts.id;
  this.state = { connected: false, isReconnecting: false };

  this._lastRecievedMap = {};
  this._sequence = 0;
}

util.inherits(RPCInterface, EventEmitter);

RPCInterface.prototype._recieved = function _recieved(originNodeId, packet) {
  if (
    this._lastRecievedMap[originNodeId] == null ||
    this._lastRecievedMap[originNodeId] < packet.sequence
  ) {
    this._lastRecievedMap[originNodeId] = packet.sequence;
    this.emit("recieved", originNodeId, packet.data);
  }
};

RPCInterface.prototype._connected = function _connected() {
  this.state.connected = true;
  this.emit("connected");
};

RPCInterface.prototype._disconnected = function _disconnected() {
  this.state.connected = false;
  this.emit("disconnected");
};

RPCInterface.prototype.connect = function connect() {
  if (typeof this._connect === "function") {
    return this._connect();
  }
};

RPCInterface.prototype.disconnect = function disconnect() {
  this._broadcast = _.noop;
  this._send = _.noop;
  this._recieved = _.noop;

  if (typeof this._disconnect === "function") {
    return this._disconnect();
  }
};

RPCInterface.prototype.broadcast = function broadcast(data) {
  if (typeof this._broadcast === "function") {
    return this._broadcast(this._createPacket(data));
  }
};

RPCInterface.prototype.send = function send(nodeId, data) {
  if (typeof this._send === "function") {
    return this._send(nodeId, this._createPacket(data));
  }
};

RPCInterface.prototype._createPacket = function createPacket(data) {
  var seq = this._sequence;
  this._sequence = this._sequence + 1;
  return {
    data: data,
    sequence: seq,
  };
};

module.exports = RPCInterface;
