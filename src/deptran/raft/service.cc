
#include "../marshallable.h"
#include "service.h"
#include <algorithm>
#include "server.h"

namespace janus {

RaftServiceImpl::RaftServiceImpl(TxLogServer *sched)
    : svr_((RaftServer*)sched) {
	struct timespec curr_time;
	clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
	srand(curr_time.tv_nsec);
}


void RaftServiceImpl::HandleRequestVote(const uint64_t& candidateTerm,
                                        const uint64_t& candidateId,
                                        const uint64_t& lastLogIndex,
                                        const uint64_t& lastLogTerm,
                                        uint64_t *currentTerm,
                                        bool_t *vote_granted,
                                        rrr::DeferredReply* defer) 
                                        {
  /* Your code here */

  Log_info("HandleRequestVote called on server %d: candidateId=%lu, candidateTerm=%lu, lastLogIndex=%lu, lastLogTerm=%lu, myTerm=%d, votedFor=%d", 
           svr_->loc_id_, candidateId, candidateTerm, lastLogIndex, lastLogTerm, svr_->currentTerm, svr_->votedFor);

  //Universal-term check, the paper says we do this for ANY request/response RPC received
  if (candidateTerm > svr_ -> currentTerm) {
    svr_ -> convertToFollower(candidateTerm);
  }

  //CHECK 1 - is this a stale request?
  bool isStaleRequest = candidateTerm < svr_ -> currentTerm;
  //CHECK 2 - Have I already voted for someone?
  bool alreadyVotedForDifferentServer = svr_ -> votedFor != -1 && svr_ -> votedFor != candidateId;
  //CHECK 3 - Are the candidate's logs at least AS up-to-date as mine?
  uint64_t myLastLogTerm = svr_ -> logs.empty() ? 0 : svr_ -> logs.back().term;
  uint64_t myLastLogIndex = svr_ -> logs.size();
  bool areLogsStale = (lastLogTerm < myLastLogTerm) || (lastLogTerm == myLastLogTerm && lastLogIndex < myLastLogIndex);

  if (isStaleRequest || alreadyVotedForDifferentServer || areLogsStale) {
    *vote_granted = false;
    *currentTerm = svr_ -> currentTerm;
    defer->reply();
    return;
  } 

  //grant the vote!
  svr_ -> votedFor = candidateId;
  svr_ -> lastHeartbeatTime = std::chrono::steady_clock::now();
  // svr_ -> resetElectionTimeout(); - we will only do this when starting elections mainly
  *vote_granted = true;
  *currentTerm = svr_ -> currentTerm;
  defer->reply();
  return;
}

void RaftServiceImpl::HandleAppendEntries(const uint64_t& term,
                                          const uint64_t& leaderId,
                                          const uint64_t& prevLogIndex,
                                          const uint64_t& prevLogTerm,
                                          const vector<LogStruct>& entries,
                                          const uint64_t& leaderCommit,
                                          uint64_t* currentTerm,
                                          bool_t* followerAppendOK,
                                          rrr::DeferredReply* defer) {
  /* Your code here */

  // Log_info("RECEIVED AE: Server %d got AE from leader %d, term=%lu, prevIdx=%lu", 
  //   svr_->loc_id_, leaderId, term, prevLogIndex);
    
  //Universal-term check, the paper says we do this for ANY request/response RPC received
  if (term > svr_ -> currentTerm) {
    svr_ -> convertToFollower(term);
  }
                                    

  //reject it if the term is lower
  if (term < svr_ -> currentTerm) {
    *currentTerm = svr_ -> currentTerm;
    *followerAppendOK = false;
    defer -> reply();
    return;
  }

  //update heartbeat timestamp on any valid AE.. 
  svr_->lastHeartbeatTime = std::chrono::steady_clock::now();
  
  //here if the prevLogIndex is either bigger than what we have in the logs
  //or if we have a different term at that index, we return false, let the leader drop a count and come back to us..
  if (prevLogIndex > 0) {
    if (prevLogIndex > svr_ -> logs.size() || 
    svr_ -> logs[prevLogIndex - 1].term != prevLogTerm) {
      *currentTerm = svr_->currentTerm;
      *followerAppendOK = false;
      defer->reply();
      return;
    }
  }
  
  
    //okay past this point the term is equal to what I have, 
    // or at least I have updated myself to be a follower in this term
    //POINT BEING - we now trust the leader, we take its logs and update ourselves accordinly, no questions asked!!
    if (svr_->serverState == RaftServer::CANDIDATE) {
      svr_->serverState = RaftServer::FOLLOWER;
    }


   uint64_t firstNewEntryIndex = prevLogIndex + 1;

   int64_t conflictIndex  = -1;
   for (uint64_t i = 0 ; i < entries.size() ; i ++ ) {
    uint64_t log_index = firstNewEntryIndex + i;
    if (log_index > svr_->logs.size() || svr_->logs[log_index - 1].term != entries[i].term) {
      conflictIndex = static_cast<int64_t> (log_index);
      break;
    }
   }

  if (conflictIndex != -1) {
    svr_ -> logs.resize(static_cast<size_t>(conflictIndex - 1));

    size_t offset =static_cast<size_t>(conflictIndex - firstNewEntryIndex);
    svr_ -> logs.insert(svr_->logs.end(), entries.begin() +offset, entries.end());
  }

  if (leaderCommit > svr_->commitIndex) {
    uint64_t lastNewEntryIndex = svr_->logs.size();
    svr_->commitIndex = std::min(leaderCommit, lastNewEntryIndex);
  }
  

  // Set return values and reply
  *currentTerm = svr_->currentTerm;
  *followerAppendOK = true;
  defer->reply();

  
}

void RaftServiceImpl::HandleHelloRpc(const string& req,
                                     string* res,
                                     rrr::DeferredReply* defer) {
  /* Your code here */
  Log_info("receive an rpc: %s", req.c_str());
  *res = "world";
  defer->reply();
}

} // namespace janus;
