
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
                                std::function<void(bool_t, uint64_t)> handleVoteResponse) {
  /*
   * Example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [handleVoteResponse, site_id](Future* fu) {
        /* this is a handler that will be invoked when the RPC returns */
        uint64_t returnedTerm;
        bool_t vote_granted;
        /* retrieve RPC return values in order */
        fu->get_reply() >> returnedTerm;
        fu->get_reply() >> vote_granted;
        Log_info("SendRequestVote: Received response from server %d, returnedTerm=%lu, voteGranted=%d", site_id, returnedTerm, vote_granted);
        /* process the RPC response here */
        if (handleVoteResponse) {
          handleVoteResponse(vote_granted, returnedTerm);
        }

      };
      /* Always use Call_Async(proxy, RPC name, RPC args..., fuattr)
      * to asynchronously invoke RPCs */
      
      // Log_info("[COMMO] SendRequestVote: Sending RequestVote to server %d with term=%lu, candidateId=%lu, lastLogIndex=%lu, lastLogTerm=%lu",
      //          site_id, candidateTerm, candidateId, lastLogIndex, lastLogTerm);
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
                                  std::vector<shared_ptr<Marshallable>> command,
                                  uint64_t leaderCommit,
                                  std::function<void(bool_t, uint64_t, uint64_t)> handleAppendResponse) {
  /*
   * More example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */

  //  Log_info("flag 1 - server %d - target %d", raftServer -> loc_id_, (site_id + 1));
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [handleAppendResponse, site_id](Future* fu) {
        uint64_t currentTerm;
        bool_t followerAppendOK;
        fu->get_reply() >> currentTerm;
        fu->get_reply() >> followerAppendOK;
        // Log_info("SendAppendEntries: Received response from server %d, currentTerm=%lu, followerAppendOK=%d", site_id, currentTerm, followerAppendOK);
        // Log_info("flag 5 - server %d - target %d (APPEND ENTRY RESPONSE RCVD)", raftServer -> loc_id_, (site_id));
        if (handleAppendResponse) {
          handleAppendResponse(followerAppendOK, currentTerm, site_id);
        }

      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      // Log_info("flag 4 - server %d - target %d (SENDING APPEND ENTRIES)", raftServer -> loc_id_, (site_id ));

      std::vector<MarshallDeputy> marshallDeputyVec;
      for (auto& cmd : command) marshallDeputyVec.emplace_back(cmd);

      Call_Async(proxy, AppendEntries, term, leaderId, prevLogIndex, prevLogTerm, marshallDeputyVec, leaderCommit, fuattr);
    }
  }
  return;
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
