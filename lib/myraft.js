var util = require("util");
var Promise = require("bluebird");
var _ = require("lodash");
var EventEmitter = require("events").EventEmitter;
var uuid = require("uuid");
var once = require("once");
const winston = require("winston");

var STATES = {
  CANDIDATE: "CANDIDATE",
  LEADER: "LEADER",
  FOLLOWER: "FOLLOWER",
};

var RPC_TYPE = {
  REQUEST_VOTE: "REQUEST_VOTE",
  REQUEST_VOTE_REPLY: "REQUEST_VOTE_REPLY",
  APPEND_ENTRIES: "APPEND_ENTRIES",
  APPEND_ENTRIES_REPLY: "APPEND_ENTRIES_REPLY",
  APPEND_ENTRY: "APPEND_ENTRY",
  DISPATCH: "DISPATCH",
  DISPATCH_SUCCESS: "DISPATCH_SUCCESS",
  DISPATCH_ERROR: "DISPATCH_ERROR",
};

const myLogger = winston.createLogger({
  level: "info",
  transports: [new winston.transports.Console()],
});

Promise.config({
  warnings: { wForgottenReturn: false },
});

function MyRaft(opts) {
  var self = this;
  var electMin;
  var electMax;
  var heartbeatInterval;

  if (opts.electionTimeout) {
    electMin = opts.electionTimeout.min;
    electMax = opts.electionTimeout.max;
  } else {
    electMin = 300;
    electMax = 500;
  }

  this._clusterSize = opts.clusterSize;
  this._unlockTimeout = opts.unlockTimeout || undefined;
  heartbeatInterval = opts.heartbeatInterval || 50;

  this.id = opts.id;
  this._closed = false;

  opts.channel.connect();
  this._channel = opts.channel;

  this._emitter = new EventEmitter();
  this._emitter.setMaxListeners(100);

  this._state = STATES.FOLLOWER;
  this._leader = null;

  this._currentTerm = 0;
  this._votedFor = null;
  this._log = [];
  this._commitIndex = -1;
  this._lastApplied = -1;

  this._emitter.on("appended", function () {
    self.emit.apply(
      self,
      ["appended"].concat(Array.prototype.slice.call(arguments))
    );
  });

  this._emitter.on("committed", function () {
    self.emit.apply(
      self,
      ["committed"].concat(Array.prototype.slice.call(arguments))
    );
  });

  this._emitter.on("leaderElected", function () {
    self.emit.apply(
      self,
      ["leaderElected"].concat(Array.prototype.slice.call(arguments))
    );
  });

  this._nextIndex = {};
  this._matchIndex = {};

  this._votes = {};

  this._lastCommunicationTimestamp = Date.now();

  this._generateRandomElectionTimeout =
    function _generateRandomElectionTimeout() {
      return _.random(electMin, electMax, false);
    };

  this._beginHeartbeat = function _beginHeartbeat() {
    var sendHeartbeat;

    _.each(self._votes, function (v, nodeId) {
      myLogger.debug(self.id + " - Sending AppendEntries/Heartbeat");

      self._channel.send(nodeId, {
        type: RPC_TYPE.APPEND_ENTRIES,
        term: self._currentTerm,
        leaderId: self.id,
        prevLogIndex: -1,
        prevLogTerm: -1,
        entries: [],
        leaderCommit: self._commitIndex,
      });
    });

    clearInterval(self._leaderHeartbeatInterval);

    sendHeartbeat = function sendHeartbeat() {
      _.each(self._votes, function (v, nodeId) {
        var entriesToSend = [];
        var prevLogIndex = -1;
        var prevLogTerm = -1;

        if (self._nextIndex[nodeId] == null) {
          self._nextIndex[nodeId] = self._log.length;
        }

        for (
          var i = self._nextIndex[nodeId], ii = self._log.length;
          i < ii;
          ++i
        ) {
          entriesToSend.push(_.extend({ index: i }, self._log[i]));
        }

        if (entriesToSend.length > 0) {
          prevLogIndex = entriesToSend[0].index - 1;

          if (prevLogIndex > -1) {
            prevLogTerm = self._log[prevLogIndex].term;
          }
        }

        myLogger.debug(self.id + " - Sending AppendEntries/Heartbeat");

        self._channel.send(nodeId, {
          type: RPC_TYPE.APPEND_ENTRIES,
          term: self._currentTerm,
          leaderId: self.id,
          prevLogIndex: prevLogIndex,
          prevLogTerm: prevLogTerm,
          entries: entriesToSend,
          leaderCommit: self._commitIndex,
        });
      });
    };

    self._leaderHeartbeatInterval = setInterval(
      sendHeartbeat,
      heartbeatInterval
    );
  };

  this._onMessageRecieved = _.bind(this._onMessageRecieved, this);
  this._channel.on("recieved", this._onMessageRecieved);

  this._emitter.on("dirty", function () {});

  this._resetElectionTimeout();
}

util.inherits(MyRaft, EventEmitter);

MyRaft.prototype._onMessageRecieved = function _onMessageRecieved(
  originNodeId,
  data
) {
  var self = this;
  var i;
  var ii;

  self._resetElectionTimeout();

  self._handleMessage(originNodeId, data);

  if (self._state === STATES.LEADER) {
    var highestPossibleCommitIndex = -1;

    for (i = self._commitIndex + 1, ii = self._log.length; i < ii; ++i) {
      if (
        self._log[i].term === self._currentTerm &&
        _.filter(self._matchIndex, function (matchIndex) {
          return matchIndex >= i;
        }).length > Math.ceil(self._clusterSize / 2)
      ) {
        highestPossibleCommitIndex = i;
      }
    }

    if (highestPossibleCommitIndex > self._commitIndex) {
      self._commitIndex = highestPossibleCommitIndex;

      self._emitter.emit("dirty");
    }
  }

  for (i = self._lastApplied + 1, ii = self._commitIndex; i <= ii; ++i) {
    self._emitter.emit(
      "committed",
      JSON.parse(JSON.stringify(self._log[i])),
      i
    );
    self._lastApplied = i;
  }
};

MyRaft.prototype.getLog = function () {
  return this._log;
};

MyRaft.prototype.getCommitIndex = function () {
  return this._commitIndex;
};

MyRaft.prototype.append = function append(data, timeout, cb) {
  var self = this;
  var msgId = self.id + "_" + uuid.v4();
  var performRequest;
  var p;

  if (typeof timeout === "function") {
    cb = timeout;
    timeout = -1;
  }

  timeout = typeof timeout === "number" ? timeout : -1;

  performRequest = once(function performRequest() {
    var entry;

    if (self._state === STATES.LEADER) {
      entry = {
        term: self._currentTerm,
        data: data,
        id: msgId,
      };

      self._log.push(entry);
      self._emitter.emit("appended", entry, self._log.length - 1);
    } else {
      self._channel.send(self._leader, {
        type: RPC_TYPE.APPEND_ENTRY,
        data: data,
        id: msgId,
      });
    }
  });

  if (self._state === STATES.LEADER) {
    performRequest();
  } else if (self._state === STATES.FOLLOWER && self._leader != null) {
    performRequest();
  } else {
    self._emitter.once("leaderElected", performRequest);
  }

  p = new Promise(function (resolve, reject) {
    var resolveOnCommitted = function _resolveOnCommitted(entry) {
      if (entry.id === msgId) {
        resolve();
        cleanup();
      }
    };
    var cleanup = function _cleanup() {
      self._emitter.removeListener("leaderElected", performRequest);
      self._emitter.removeListener("committed", resolveOnCommitted);
      clearTimeout(timeoutHandle);
    };
    var timeoutHandle;

    self._emitter.on("committed", resolveOnCommitted);

    if (timeout > -1) {
      timeoutHandle = setTimeout(function _failOnTimeout() {
        reject(new Error("Timed out before the entry was committed"));
        cleanup();
      }, timeout);
    }
  });

  if (cb != null) {
    p.then(_.bind(cb, null, null)).catch(cb);
  } else {
    return p;
  }
};

MyRaft.prototype.isLeader = function isLeader() {
  return this._state === STATES.LEADER;
};

MyRaft.prototype.hasUncommittedEntriesInPreviousTerms =
  function hasUncommittedEntriesInPreviousTerms() {
    var self = this;

    return (
      _.find(self._log, function (entry, idx) {
        return entry.term < self._currentTerm && idx > self._commitIndex;
      }) != null
    );
  };

MyRaft.prototype._handleMessage = function _handleMessage(originNodeId, data) {
  var self = this;
  var conflictedAt = -1;
  var entry;

  if (data.term > self._currentTerm) {
    self._currentTerm = data.term;
    self._leader = null;
    self._votedFor = null;
    self._state = STATES.FOLLOWER;
    clearInterval(self._leaderHeartbeatInterval);
  }

  switch (data.type) {
    case RPC_TYPE.REQUEST_VOTE:
      if (data.term >= self._currentTerm) {
        var lastLogEntry = _.last(self._log);
        var lastLogTerm = lastLogEntry != null ? lastLogEntry.term : -1;
        var candidateIsAtLeastAsUpToDate =
          data.lastLogTerm > lastLogTerm ||
          (data.lastLogTerm === lastLogTerm &&
            data.lastLogIndex >= self._log.length - 1);

        if (
          (self._votedFor == null || self._votedFor === data.candidateId) &&
          candidateIsAtLeastAsUpToDate
        ) {
          self._votedFor = data.candidateId;
          self._channel.send(data.candidateId, {
            type: RPC_TYPE.REQUEST_VOTE_REPLY,
            term: self._currentTerm,
            voteGranted: true,
          });
          return;
        }
      }

      self._channel.send(originNodeId, {
        type: RPC_TYPE.REQUEST_VOTE_REPLY,
        term: self._currentTerm,
        voteGranted: false,
      });
      break;

    case RPC_TYPE.REQUEST_VOTE_REPLY:
      if (data.term === self._currentTerm && data.voteGranted === true) {
        myLogger.debug(this.id + " - Got vote from " + originNodeId);

        self._votes[originNodeId] = true;
        if (
          self._state === STATES.CANDIDATE &&
          _.values(self._votes).length > Math.ceil(this._clusterSize / 2)
        ) {
          myLogger.debug(this.id + " - I am Leader");

          self._state = STATES.LEADER;
          self._nextIndex = {};
          self._matchIndex = {};
          self._beginHeartbeat();
          self._emitter.emit("leaderElected");
        }
      }
      break;

    case RPC_TYPE.APPEND_ENTRY:
      if (self._state === STATES.LEADER) {
        entry = {
          term: self._currentTerm,
          id: data.id,
          data: data.data,
        };

        self._log.push(entry);
        self._emitter.emit("appended", entry, self._log.length - 1);

        self._emitter.emit("dirty");
      }
      break;

    case RPC_TYPE.APPEND_ENTRIES:
      if (data.term >= self._currentTerm && self._state !== STATES.LEADER) {
        self._state = STATES.FOLLOWER;
        self._leader = originNodeId;
        self._emitter.emit("leaderElected");
      }

      if (data.term < self._currentTerm) {
        myLogger.debug(
          this.id + " - Got AppendEntries/Heartbeat with older term"
        );

        self._channel.send(originNodeId, {
          type: RPC_TYPE.APPEND_ENTRIES_REPLY,
          term: self._currentTerm,
          success: false,
        });
        return;
      }

      if (
        data.prevLogIndex > -1 &&
        (self._log[data.prevLogIndex] == null ||
          self._log[data.prevLogIndex].term !== data.prevLogTerm)
      ) {
        myLogger.debug(
          this.id + " - Got AppendEntries/Heartbeat with prev log mismatch"
        );

        self._channel.send(originNodeId, {
          type: RPC_TYPE.APPEND_ENTRIES_REPLY,
          term: self._currentTerm,
          success: false,
        });
        return;
      }

      _.each(data.entries, function (entry) {
        var idx = entry.index;

        if (self._log[idx] != null && self._log[idx].term !== entry.term) {
          conflictedAt = idx;
        }
      });

      if (conflictedAt > 0) {
        myLogger.debug(
          this.id +
            " - Got AppendEntries/Heartbeat; conflict at index" +
            conflictedAt
        );
        self._log = self._log.slice(0, conflictedAt);
      }

      _.each(data.entries, function (entry) {
        var idx = entry.index;

        if (self._log[idx] == null) {
          self._log[idx] = {
            term: entry.term,
            data: entry.data,
            id: entry.id,
          };
        }
      });

      if (data.leaderCommit > self._commitIndex) {
        self._commitIndex = Math.min(data.leaderCommit, self._log.length - 1);
      }

      self._channel.send(originNodeId, {
        type: RPC_TYPE.APPEND_ENTRIES_REPLY,
        term: self._currentTerm,
        success: true,
        lastLogIndex: self._log.length - 1,
      });

      break;

    case RPC_TYPE.APPEND_ENTRIES_REPLY:
      if (self._state === STATES.LEADER && self._currentTerm === data.term) {
        if (data.success === true && data.lastLogIndex > -1) {
          self._nextIndex[originNodeId] = data.lastLogIndex + 1;
          self._matchIndex[originNodeId] = data.lastLogIndex;
        } else {
          if (self._nextIndex[originNodeId] == null) {
            self._nextIndex[originNodeId] = self._log.length;
          }

          self._nextIndex[originNodeId] = Math.max(
            self._nextIndex[originNodeId] - 1,
            0
          );
        }
      }
      break;

    case RPC_TYPE.DISPATCH_ERROR:
      var err = new Error(data.message);
      err.stack = data.stack;
      self._emitter.emit.apply(self._emitter, ["rpcError", data.id, err]);
      break;

    case RPC_TYPE.DISPATCH_SUCCESS:
      self._emitter.emit.apply(self._emitter, [
        "rpcSuccess",
        data.id,
        data.returnValue,
      ]);
      break;
  }
};

MyRaft.prototype._resetElectionTimeout = function _resetElectionTimeout() {
  var self = this;
  var timeout = self._generateRandomElectionTimeout();

  if (self._electionTimeout != null) {
    clearTimeout(self._electionTimeout);
  }

  self._electionTimeout = setTimeout(function () {
    self._resetElectionTimeout();
    self._beginElection();
  }, timeout);
};

MyRaft.prototype._beginElection = function _beginElection() {
  var lastLogIndex;

  this._currentTerm = this._currentTerm + 1;
  this._state = STATES.CANDIDATE;
  this._leader = null;
  this._votedFor = this.id;
  this._votes = {};

  lastLogIndex = this._log.length - 1;

  myLogger.debug(this.id + " - Beginning election");

  this._channel.broadcast({
    type: RPC_TYPE.REQUEST_VOTE,
    term: this._currentTerm,
    candidateId: this.id,
    lastLogIndex: lastLogIndex,
    lastLogTerm: lastLogIndex > -1 ? this._log[lastLogIndex].term : -1,
  });
};

MyRaft.prototype.close = function close(cb) {
  var self = this;
  var p;

  this._channel.removeListener("recieved", this._onMessageRecieved);
  clearTimeout(this._electionTimeout);
  clearInterval(this._leaderHeartbeatInterval);

  this._emitter.removeAllListeners();

  p = new Promise(function (resolve, reject) {
    self._channel.once("disconnected", function () {
      resolve();
    });
    self._channel.disconnect();
  });

  if (cb != null) {
    p.then(_.bind(cb, null, null)).catch(cb);
  } else {
    return p;
  }
};

module.exports = MyRaft;
module.exports._STATES = _.cloneDeep(STATES);
module.exports._RPC_TYPE = _.cloneDeep(RPC_TYPE);
