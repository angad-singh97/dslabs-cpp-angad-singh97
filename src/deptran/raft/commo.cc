
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "server.h"
#include "raft_rpc.h"
#include "macros.h"

namespace janus {

RaftCommo::RaftCommo(PollMgr* poll) : Communicator(poll) {
}


//sendRequestVote(candidateTerm, candidateId, lastLogIndex, lastLogTerm) -> term, voteGranted

void RaftCommo::SendRequestVote(parid_t par_id,
                                siteid_t site_id,
                                uint64_t candidateTerm,
                                uint64_t candidateId,
                                uint64_t lastLogIndex,
                                uint64_t lastLogTerm,
                                RaftServer* raftServer) {
  /*
   * Example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [raftServer, site_id](Future* fu) {
        /* this is a handler that will be invoked when the RPC returns */
        uint64_t returnedTerm;
        bool_t vote_granted;
        /* retrieve RPC return values in order */
        fu->get_reply() >> returnedTerm;
        fu->get_reply() >> vote_granted;
        Log_info("SendRequestVote: Received response from server %d, returnedTerm=%lu, voteGranted=%d", site_id, returnedTerm, vote_granted);
        /* process the RPC response here */
        raftServer -> handleVoteResponse(vote_granted, returnedTerm);

      };
      /* Always use Call_Async(proxy, RPC name, RPC args..., fuattr)
      * to asynchronously invoke RPCs */
      Log_info("[COMMO] SendRequestVote: Sending RequestVote to server %d with term=%lu, candidateId=%lu, lastLogIndex=%lu, lastLogTerm=%lu",
               site_id, candidateTerm, candidateId, lastLogIndex, lastLogTerm);
      Call_Async(proxy, RequestVote, candidateTerm, candidateId, lastLogIndex, lastLogTerm, fuattr);
    }
  }
}

void RaftCommo::SendAppendEntries(parid_t par_id,
                                  siteid_t site_id,
                                  uint64_t term,
                                  uint64_t leaderId,
                                  uint64_t prevLogIndex,
                                  uint64_t prevLogTerm,
                                  vector<LogStruct> entries,
                                  uint64_t leaderCommit,
                                  RaftServer* raftServer) {
  /*
   * More example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [raftServer, site_id](Future* fu) {
        uint64_t currentTerm;
        bool_t followerAppendOK;
        fu->get_reply() >> currentTerm;
        fu->get_reply() >> followerAppendOK;
        raftServer -> handleAppendResponse(followerAppendOK, currentTerm, site_id);

      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      Call_Async(proxy, AppendEntries, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, fuattr);
    }
  }
}

shared_ptr<IntEvent> 
RaftCommo::SendString(parid_t par_id, siteid_t site_id, const string& msg, string* res) {
  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [res,ev](Future* fu) {
        fu->get_reply() >> *res;
        ev->Set(1);
      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      Call_Async(proxy, HelloRpc, msg, fuattr);
    }
  }
  return ev;
}


} // namespace janus
