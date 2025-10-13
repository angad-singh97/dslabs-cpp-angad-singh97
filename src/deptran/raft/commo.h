#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"

namespace janus {

class TxData;

class RaftServer;
// class LogStruct;
class RaftCommo : public Communicator {


 public:
  RaftCommo() = delete;
  RaftCommo(PollMgr*);



  void SendRequestVote(parid_t par_id,
                      siteid_t site_id,
                      uint64_t candidateTerm,
                      uint64_t candidateId,
                      uint64_t lastLogIndex,
                      uint64_t lastlogTerm,
                      std::function<void(bool_t, uint64_t)> handleVoteResponse);

  void SendAppendEntries(parid_t par_id,
                         siteid_t site_id,
                         uint64_t term,
                         uint64_t leaderId,
                         uint64_t prevLogIndex,
                         uint64_t prevLogTerm,
                         std::vector<shared_ptr<Marshallable>> command,
                         uint64_t leaderCommit,
                         std::function<void(bool_t, uint64_t, uint64_t)> handleAppendResponse);

  shared_ptr<IntEvent> 
  SendString(parid_t par_id, siteid_t site_id, const string& msg, string* res);

  /* Do not modify this class below here */

  friend class FpgaRaftProxy;
 public:
#ifdef RAFT_TEST_CORO
  std::recursive_mutex rpc_mtx_ = {};
  uint64_t rpc_count_ = 0;
#endif
};

} // namespace janus

