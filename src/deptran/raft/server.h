#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include <chrono>
#include "commo.h"

namespace janus {

#define HEARTBEAT_INTERVAL 250000

struct LogStruct {
  shared_ptr<Marshallable> cmd;
  uint64_t term;
};



class RaftServer : public TxLogServer {
 public:
  /* Your data here */
  //persistent state variables
  std::atomic<int> currentTerm {0};
  std::atomic<int>  votedFor{-1};
  static const int SERVER_COUNT = 5;
  std::mutex logs_mutex;
  std::vector<LogStruct> logs;

  
  //volatile state variables
  std::atomic<int>  commitIndex {0};
  std::atomic<int>  lastApplied {0};
  
  enum ServerState {FOLLOWER, LEADER, CANDIDATE};
  std::atomic<ServerState> serverState {RaftServer::FOLLOWER};
  
  //right now there is no difference in my implementation between them
  
  //volatile state variables - only for leaders to use for sending data
  std::mutex state_mutex;
  std::mutex start_mutex;
  int nextIndex[SERVER_COUNT];
  int matchIndex[SERVER_COUNT];


  //holding track of election timeouts here
  std::atomic<std::chrono::steady_clock::time_point> lastHeartbeatTime;
  std::atomic<std::chrono::milliseconds> electionTimeout;

  //tracking the election stuff here
  std::atomic<int> votesReceived {0};


  std::mutex vote_response_mutex;
  std::mutex append_response_mutex;
  std::mutex state_transition_mutex;
  std::mutex commit_processing_mutex;


  /* Your functions here */

  void resetElectionTimeout();
  void extendElectionTimeout();
  void startElection();
  void handleVoteResponse(bool voteGranted, uint64_t returnedTerm);
  void handleAppendResponse(bool success, uint64_t returnedTerm, int followerId, int sentUpToIndex);
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
