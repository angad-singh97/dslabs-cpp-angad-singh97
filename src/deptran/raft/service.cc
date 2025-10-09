
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

  //IF THE VOTE HAS COME IN FROM SOMEONE WITH A GREATER TERM
      //UPDATE YOUR CURRENT TERM
      //REVERT TO FOLLOWER
 if (candidateTerm > svr_->currentTerm) {
   svr_ -> currentTerm = candidateTerm;
   svr_ -> votedFor = -1;
   svr_ -> serverState = RaftServer::FOLLOWER;
   //ack the new leader candidate, give them some time to win.. 
   // we can always try again later, right?
   svr_ -> resetElectionTimeout(); 
 }

 //MAIN DECISION LOGIC HERE

 //REPLY FALSE IF CANDIDATE TERM < CURRENT TERM
 if (candidateTerm < svr_ -> currentTerm) {
  *currentTerm = svr_ -> currentTerm;
  *vote_granted = false;
  defer->reply();
  return;
 }

 //GRANT THE VOTE, IF 
    //VOTED FOR IS NULL (-1) OR EQUALS CANDIDATE_ID 
    bool votedForCondition = svr_ -> votedFor  == -1 || svr_ -> votedFor == candidateId;
    
    // /(does this mean we already voted for them? why are we not checking things like term? does this get checked by the log check?)
    //AND
    //CANDIDATE'S LOG IS AT LEAST AS UP-TO-DATE AS OURS
    bool logCheckCondition;
    vector<LogStruct> localLogs = svr_ -> logs;
    int localLogLastIndex = localLogs.empty() ? 0 : localLogs.size();
    int localLogLastTerm = localLogs.empty()? 0 : localLogs.back().term;

    if (lastLogTerm > localLogLastTerm || (lastLogTerm == localLogLastTerm && lastLogIndex >= localLogLastIndex)) {
      logCheckCondition = true;
    } else {
      logCheckCondition = false;
    }



    //decide basis both conditions now
    if (votedForCondition && logCheckCondition) {
      svr_ -> votedFor = candidateId;
      *currentTerm = svr_ -> currentTerm;
      *vote_granted = true;
    } else {
      *currentTerm = svr_ -> currentTerm;
      *vote_granted = false;
    }


    defer->reply();
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
    svr_ -> currentElectionTerm = 0;
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
