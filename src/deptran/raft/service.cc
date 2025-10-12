
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
  if (term < svr_ -> currentTerm) {
    *currentTerm = svr_ -> currentTerm;
    *followerAppendOK = false;
    defer -> reply();
    return;
  }

  if (prevLogIndex > 0) {
    if (prevLogIndex > svr_ -> logs.size() || 
    svr_ -> logs[prevLogIndex - 1].term != prevLogTerm) {
      *currentTerm = svr_->currentTerm;
      *followerAppendOK = false;
      defer->reply();
      return;
    }
  }

  //if I received one of these from someone with a higher term, I should become a follower
  if (term > svr_->currentTerm) {
    svr_->currentTerm = term;
    svr_->serverState = RaftServer::FOLLOWER;
    svr_->votedFor = -1;

    svr_ -> electionInProgress = false;
    svr_ -> votesReceived = 0;
  }

  //this is a heartbeat, so reset the timer
  svr_->resetElectionTimeout();
  svr_->lastHeartbeatTime = std::chrono::steady_clock::now();

  svr_->serverState = RaftServer::FOLLOWER;

  int conflictingIndex = prevLogIndex;

  for (int i = 0; i < entries.size(); i++) {
    conflictingIndex++;
    if (conflictingIndex <= svr_ -> logs.size()) {
      if (svr_->logs[conflictingIndex - 1].term != entries[i].term) {
        svr_->logs.erase(svr_->logs.begin() + conflictingIndex - 1, svr_->logs.end());
        // append all remaining entries starting at i
        for (int j = i; j < entries.size(); j++) svr_->logs.push_back(entries[j]);
        goto done_append;
      }
    } else {
      // log is shorter; append remaining from i
      for (int j = i; j < entries.size(); j++) svr_->logs.push_back(entries[j]);
      goto done_append;
    }
  }
  
  // if we got here, either no entries or all matched existing; nothing to append
  
  done_append: ;

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
