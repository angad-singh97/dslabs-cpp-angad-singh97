#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include <chrono>
#include "commo.h"

namespace janus {

#define HEARTBEAT_INTERVAL 100000

class LogStruct  : public Marshallable {
  public:
    int term;
    shared_ptr<Marshallable> command;

    LogStruct(): Marshallable(MarshallDeputy::CONTAINER_CMD) {}

    Marshal& ToMarshal(Marshal& m) const override {
      m << term;
      MarshallDeputy md(command);
      m << md;
      return m;
    }

    Marshal& FromMarshal (Marshal &m) override {
      m >> term;
      MarshallDeputy md;
      m >> md;
      command = md.sp_data_; // here we get the actual data out
      return m;
    }

};

// Provide marshal operators for LogStruct so std::vector<LogStruct> can be serialized
inline Marshal& operator<<(Marshal& m, const LogStruct& ls) {
  return ls.ToMarshal(m);
}
inline Marshal& operator>>(Marshal& m, LogStruct& ls) {
  return ls.FromMarshal(m);
}

class RaftServer : public TxLogServer {
 public:
  /* Your data here */
  //persistent state variables
  int currentTerm = 0;
  int votedFor = -1;
  static const int SERVER_COUNT = 5;

  //volatile state variables
  int commitIndex = 0;
  int lastApplied = 0;

  enum ServerState {FOLLOWER, LEADER, CANDIDATE};
  ServerState serverState = RaftServer::FOLLOWER;

  //right now there is no difference in my implementation between them

  //volatile state variables - only for leaders to use for sending data
  int nextIndex[SERVER_COUNT];
  int matchIndex[SERVER_COUNT];

  vector<LogStruct> logs;

  //holding track of election timeouts here
  std::chrono::steady_clock::time_point lastHeartbeatTime;
  std::chrono::milliseconds electionTimeout;

  //tracking the election stuff here
  int votesReceived = 0;


  /* Your functions here */

  void resetElectionTimeout();
  void startElection();
  void handleVoteResponse(bool voteGranted, uint64_t returnedTerm);
  void handleAppendResponse(bool success, uint64_t returnedTerm, int followerId);
  void convertToFollower(uint64_t newTerm);

  /* do not modify this class below here */

 public:
  RaftServer(Frame *frame) ;
  ~RaftServer() ;

  bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
  void GetState(bool *is_leader, uint64_t *term);

 private:
  bool disconnected_ = false;
	void Setup();

 public:
  void SyncRpcExample();
  void Disconnect(const bool disconnect = true);
  void Reconnect() {
    Disconnect(false);
  }
  bool IsDisconnected();

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };
  RaftCommo* commo() {
    return (RaftCommo*)commo_;
  }
};
} // namespace janus
