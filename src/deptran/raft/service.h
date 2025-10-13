#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "server.h"
#include "raft_rpc.h"
#include "macros.h"

class SimpleCommand;
namespace janus {

class TxLogServer;
class RaftServer;
class RaftServiceImpl : public RaftService {
 public:
  RaftServer* svr_;
  RaftServiceImpl(TxLogServer* sched);


  RpcHandler(RequestVote, 6,
             const uint64_t&, candidateTerm,
             const uint64_t&, candidateId,
             const uint64_t&, lastLogIndex,
             const uint64_t&, lastLogTerm,
             uint64_t*, currentTerm,
             bool_t*, vote_granted) {
    *currentTerm = 0;
    *vote_granted = false;
  }

  RpcHandler(AppendEntries, 7,
             const uint64_t&, term,
             const uint64_t&, leaderId,
             const uint64_t&, prevLogIndex,
             const uint64_t&, prevLogTerm,
             const uint64_t&, leaderCommit,
             uint64_t*, currentTerm,
             bool_t*, followerAppendOK) {
    *currentTerm = 0;
    *followerAppendOK = false;
  }

  RpcHandler(HelloRpc, 2, const string&, req, string*, res) {
    *res = "error"; 
  };

};

} // namespace janus
