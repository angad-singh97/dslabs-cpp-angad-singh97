
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

  Log_info("VOTE REQUEST: Server %d received vote request from candidate %d, term=%d, lastLogIndex=%d, lastLogTerm=%d, myTerm=%d, votedFor=%d", 
    svr_->loc_id_, candidateId, candidateTerm, lastLogIndex, lastLogTerm, svr_->currentTerm.load(), svr_->votedFor.load());

  //Universal-term check, the paper says we do this for ANY request/response RPC received
  if (candidateTerm > svr_ -> currentTerm.load()) {
    svr_ -> convertToFollower(candidateTerm);
  }

  //CHECK 1 - is this a stale request?
  bool isStaleRequest = candidateTerm < svr_ -> currentTerm.load();
    //CHECK 2 - Have I already voted for someone?
  bool alreadyVotedForDifferentServer = svr_ -> votedFor.load() != -1 && svr_ -> votedFor.load() != candidateId;  //CHECK 3 - Are the candidate's logs at least AS up-to-date as mine?
  uint64_t myLastLogTerm, myLastLogIndex;
  {
      std::lock_guard<std::mutex> lock(svr_->logs_mutex);
      myLastLogTerm = svr_ -> logs.empty() ? 0 : svr_ -> logs.back().first;
      myLastLogIndex = svr_ -> logs.size();
  }
  bool areLogsStale = (lastLogTerm < myLastLogTerm) || (lastLogTerm == myLastLogTerm && lastLogIndex < myLastLogIndex);

  if (isStaleRequest || alreadyVotedForDifferentServer || areLogsStale) {
    Log_info("VOTE DENIED: Server %d denying vote to candidate %d (stale=%d, alreadyVoted=%d, logsStale=%d)", 
      svr_->loc_id_, candidateId, isStaleRequest, alreadyVotedForDifferentServer, areLogsStale);
    *vote_granted = false;
    *currentTerm = svr_ -> currentTerm.load();
    defer->reply();
    return;
  } 

  //grant the vote!
  Log_info("VOTE GRANTED: Server %d granting vote to candidate %d", svr_->loc_id_, candidateId);
  svr_ -> votedFor.store(candidateId);
  svr_ -> lastHeartbeatTime.store(std::chrono::steady_clock::now());
  // svr_ -> resetElectionTimeout(); - we will only do this when starting elections mainly
  *vote_granted = true;
  *currentTerm = svr_ -> currentTerm.load();
  defer->reply();
  return;
}

void RaftServiceImpl::HandleAppendEntries(const uint64_t& term,
                                          const uint64_t& leaderId,
                                          const uint64_t& prevLogIndex,
                                          const uint64_t& prevLogTerm,
                                          const std::vector<MarshallDeputy>& marshallDeputyVec,
                                          const std::vector<uint64_t>& entry_terms,
                                          const uint64_t& leaderCommit,
                                          uint64_t* currentTerm,
                                          bool_t* followerAppendOK,
                                          rrr::DeferredReply* defer) {
  /* Your code here */

  Log_info("APPEND REQUEST: Server %d received append request from leader %d, term=%d, prevLogIndex=%d, prevLogTerm=%d, entries=%zu, myTerm=%d", 
            svr_->loc_id_, leaderId, term, prevLogIndex, prevLogTerm, marshallDeputyVec.size(), svr_->currentTerm.load());

    
  //Universal-term check, the paper says we do this for ANY request/response RPC received
  if (term > svr_ -> currentTerm.load()) {
    svr_ -> convertToFollower(term);
  }

  // Log_info("flag 1b - server (on the other side) %d (request received lol)", svr_ -> loc_id_);

                                    

  //reject it if the term is lower
  if (term < svr_ -> currentTerm.load()) {
    Log_info("APPEND REJECTED: Server %d rejecting append from leader %d (stale term %d < %d)", 
      svr_->loc_id_, leaderId, term, svr_->currentTerm.load());
    *currentTerm = svr_ -> currentTerm.load();
    *followerAppendOK = false;
    defer -> reply();
    return;
  }

  // Log_info("flag 1c - server (on the other side) %d (request received lol)", svr_ -> loc_id_);


  //update heartbeat timestamp on any valid AE.. 
  svr_->lastHeartbeatTime.store(std::chrono::steady_clock::now());
  
  //here if the prevLogIndex is either bigger than what we have in the logs
  //or if we have a different term at that index, we return false, let the leader drop a count and come back to us..
  if (prevLogIndex > 0) {
    {
      std::lock_guard<std::mutex> lock(svr_->logs_mutex);
      if (prevLogIndex > svr_ -> logs.size() || 
          svr_ -> logs[prevLogIndex - 1].first != prevLogTerm) {
          Log_info("APPEND REJECTED: Server %d rejecting append (prevLogIndex=%d > logs.size()=%zu OR prevLogTerm mismatch: %d != %d)", 
            svr_->loc_id_, prevLogIndex, svr_->logs.size(), svr_->logs[prevLogIndex - 1].first, prevLogTerm);
          *currentTerm = svr_->currentTerm.load();
          *followerAppendOK = false;
          defer->reply();
          return;
      }
    }
  }

  // Log_info("flag 1d - server (on the other side) %d (request received lol)", svr_ -> loc_id_);

  
  
    //okay past this point the term is equal to what I have, 
    // or at least I have updated myself to be a follower in this term
    //POINT BEING - we now trust the leader, we take its logs and update ourselves accordinly, no questions asked!!
    if (svr_->serverState.load() == RaftServer::CANDIDATE) {
      svr_->serverState.store(RaftServer::FOLLOWER);
    }

    // Log_info("flag 1e - server (on the other side) %d (request received lol)", svr_ -> loc_id_);



    {
      std::lock_guard<std::mutex> lock(svr_->logs_mutex);

      for (size_t i=0; i< marshallDeputyVec.size(); i++) {
        uint64_t entryLogIndex = prevLogIndex + 1 + i;

        if (entryLogIndex > svr_ -> logs.size() || 
        svr_->logs[entryLogIndex - 1].first != entry_terms[i]) {

          Log_info("CONFLICT DETECTED: Server %d truncating logs at index %d, appending %zu entries from leader", 
            svr_->loc_id_, entryLogIndex - 1, marshallDeputyVec.size() - i);
          svr_->logs.resize(entryLogIndex - 1);

          for (size_t j = i; j < marshallDeputyVec.size() ; j++) {
            svr_->logs.push_back({entry_terms[j], marshallDeputyVec[j].sp_data_});
          }

          break;
        }
      }
    }
   /*uint64_t firstNewEntryIndex = prevLogIndex + 1;

   int64_t conflictIndex  = -1;
   for (uint64_t i = 0 ; i < marshallDeputyVec.size() ; i ++ ) {
    uint64_t log_index = firstNewEntryIndex + i;
    if (log_index > svr_->logs.size() || svr_->logs[log_index - 1].first != term) {
      conflictIndex = static_cast<int64_t> (log_index);
      break;
    }
   }

  //  Log_info("flag 1f - server (on the other side) %d (request received lol)", svr_ -> loc_id_);


  if (conflictIndex != -1) {
    {
      size_t offset =static_cast<size_t>(conflictIndex - firstNewEntryIndex);
      {
        std::lock_guard<std::mutex> lock(svr_->logs_mutex);
        svr_ -> logs.resize(static_cast<size_t>(conflictIndex - 1));
        for (size_t i = offset; i < marshallDeputyVec.size(); ++i) {
          svr_->logs.push_back({term, marshallDeputyVec[i].sp_data_});
        }
      }
    }
  } else {
    // No conflict - append all new entries
    {
      std::lock_guard<std::mutex> lock(svr_->logs_mutex);
      for (size_t i = 0; i < marshallDeputyVec.size(); ++i) {
          uint64_t log_index = firstNewEntryIndex + i;
          if (log_index > svr_->logs.size()) {
              svr_->logs.push_back({term, marshallDeputyVec[i].sp_data_});
          }
      }
    }
  }*/

  // Log_info("flag 1g - server (on the other side) %d (request received lol)", svr_ -> loc_id_);


  if (leaderCommit > svr_->commitIndex.load()) {
    uint64_t lastNewEntryIndex;
    {
        std::lock_guard<std::mutex> lock(svr_->logs_mutex);
        lastNewEntryIndex = svr_->logs.size();
    }
    Log_info("COMMIT UPDATE: Server %d updating commitIndex from %d to %d", 
      svr_->loc_id_, svr_->commitIndex.load(), std::min(leaderCommit, lastNewEntryIndex));
    svr_->commitIndex.store(std::min(leaderCommit, lastNewEntryIndex));
  }
  
  // Log_info("flag 1h - server (on the other side) %d (request received lol)", svr_ -> loc_id_);


  // Set return values and reply
  Log_info("APPEND SUCCESS: Server %d successfully processed append from leader %d, logSize=%zu", 
    svr_->loc_id_, leaderId, svr_->logs.size());
  *currentTerm = svr_->currentTerm.load();
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
