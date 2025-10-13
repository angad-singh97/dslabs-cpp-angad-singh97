

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"
#include <ctime>


namespace janus {

RaftServer::RaftServer(Frame * frame) {
  frame_ = frame ;
  /* Your code here for server initialization. Note that this function is 
     called in a different OS thread. Be careful about thread safety if 
     you want to initialize variables here. */
  
  // Don't initialize timing here - loc_id_ is not set yet
  // Move initialization to Setup() where loc_id_ is properly set

}

RaftServer::~RaftServer() {
  /* Your code here for server teardown */

}

void RaftServer::Setup() {
  /* Your code here for server setup. Due to the asynchronous nature of the 
     framework, this function could be called after a RPC handler is triggered. 
     Your code should be aware of that. This function is always called in the 
     same OS thread as the RPC handlers. */
  
  // Initialize election timeout here where loc_id_ is properly set

  
  lastHeartbeatTime.store(std::chrono::steady_clock::now() - std::chrono::milliseconds(loc_id_ * 100));  
  resetElectionTimeout();
  
  // SyncRpcExample();
  Coroutine::CreateRun([this]() {
    while (true) {
      auto now = std::chrono::steady_clock::now();

      if ((serverState.load() == RaftServer::FOLLOWER || serverState.load() == RaftServer::CANDIDATE) && now >= lastHeartbeatTime.load() + electionTimeout.load()) {
        Log_info("ELECTION TIMEOUT: Server %d starting election as %s - timeout expired", 
          loc_id_, (serverState.load() == RaftServer::FOLLOWER) ? "FOLLOWER" : "CANDIDATE");
        startElection();
        resetElectionTimeout();
      }

      Coroutine::Sleep(10000);
    }

  });

  Coroutine::CreateRun([this]() {
    while (true) {
      if (serverState.load() == RaftServer::LEADER) {
        for (int i = 0 ; i < SERVER_COUNT ; i++) {
          if (i == loc_id_) continue;
          // Log_info("flag 1 - server %d - target %d", loc_id_, (i + 1));

          int currNextIndex = nextIndex[i].load();
          int prevIdx = currNextIndex - 1;
          uint64_t prevTerm = 0;
          {
            std::lock_guard<std::mutex> lock(logs_mutex);
            if (prevIdx > 0 && prevIdx <= (int)logs.size()) {
              prevTerm = logs[prevIdx - 1].first;
            }
          }
          

          vector<shared_ptr<Marshallable>> entries;
          vector<uint64_t> entry_terms; 
          int sentUpToIndex;
          {
            std::lock_guard<std::mutex> lock(logs_mutex);
            if (currNextIndex >= 1 && currNextIndex <= (int)logs.size()) {
              for (auto it = logs.begin() +(currNextIndex - 1); it!= logs.end(); ++it) {
                entries.push_back(it->second);  //We only want the command here!!!1
                entry_terms.push_back(it->first);//no, the term was also needed...
              }
            }
            sentUpToIndex= currNextIndex - 1 + entries.size();
          }

          Log_info("SENDING HEARTBEAT: Server %d sending heartbeat to server %d, term=%d, prevIdx=%d, prevTerm=%d, entries=%zu, sentUpToIndex=%d", 
            loc_id_, i, currentTerm.load(), prevIdx, prevTerm, entries.size(), sentUpToIndex);

          commo() -> SendAppendEntries(0, i, currentTerm.load(), loc_id_, prevIdx, prevTerm, entries, entry_terms, commitIndex.load(), 
          [this, i, sentUpToIndex] (bool success, uint64_t returnedTerm, uint64_t followerId) {
            Log_info("HEARTBEAT RESPONSE: Server %d received heartbeat response from server %d: success=%d, term=%d", 
              loc_id_, i, success, returnedTerm);
            handleAppendResponse(success, returnedTerm, i, sentUpToIndex);
          });
        } 
      }
      Coroutine::Sleep(HEARTBEAT_INTERVAL);
    }

  });


  Coroutine::CreateRun([this]() {
    while (true) {
      while (lastApplied.load() < commitIndex.load() && lastApplied.load() < (int)logs.size()) {
        lastApplied.fetch_add(1);
        shared_ptr<Marshallable> cmd;
        int term;
        {
          std::lock_guard<std::mutex> lock(logs_mutex);
          cmd = logs[lastApplied.load()-1].second;
          term = logs[lastApplied.load()-1].first;
        }
        if (app_next_) {
          app_next_(*cmd);
        }
        // Log_info("Applied entry %d: term=%d", lastApplied.load(), term);
      }
      Coroutine::Sleep(50); 
    }
  });
}

void RaftServer::handleVoteResponse (bool voteGranted, uint64_t returnedTerm) {



  //Universal-term check, the paper says we do this for ANY request/response RPC received
  if (returnedTerm >  currentTerm.load()) {
    // Log_info("Server %d received vote response: returnedTerm=%lu, currentTerm(before conversion)=%d", loc_id_, returnedTerm, currentTerm.load());
    convertToFollower(returnedTerm);
    // Log_info("Server %d converted to follower due to higher term in vote response. returnedTerm=%lu, currentTerm(after conversion)=%d", loc_id_, returnedTerm, currentTerm.load());
    return;
  }

  if (serverState.load() != CANDIDATE){
    // Log_info("Ignoring late vote response: server %d is now %s (was candidate)", 
    //          loc_id_, (serverState.load() == LEADER) ? "LEADER" : "FOLLOWER");
    return;
  }

  if (returnedTerm < currentTerm.load()) {
    // Log_info("handleVoteResponse: Received vote response with older term (returnedTerm=%lu < currentTerm=%d), no action taken. voteGranted=%d, serverState=%d, votesReceived=%d",
        //  returnedTerm, currentTerm.load(), voteGranted, serverState.load(), votesReceived.load());
    return;
  }

  if (voteGranted) {
    votesReceived.fetch_add(1);
    // Log_info("Received voteGranted=true. votesReceived=%d, currentTerm=%d, serverState=%d", votesReceived.load(), currentTerm.load(), serverState.load());
    int majority = (SERVER_COUNT/2) + 1;
    if (votesReceived.load() >= majority) {
      Log_info("LEADER TRANSITION: Server %d is now LEADER for term %lu", loc_id_, currentTerm.load());
      serverState.store(RaftServer::LEADER);
      votesReceived.store(0); // Reset vote counter after becoming leader

      std::lock_guard<std::mutex> lock(logs_mutex);
      int nextLogIndex = logs.size() + 1;
      for (int i = 0; i < SERVER_COUNT ; i++){
        nextIndex[i].store(nextLogIndex);
        matchIndex[i].store(0);
      }

      return;
    }
  }
}

void RaftServer::handleAppendResponse(bool success, uint64_t returnedTerm, int followerId, int sentUpToIndex) {
  // Log_info("flag 10 - server %d: handleAppendResponse", loc_id_);
  if (returnedTerm > currentTerm.load()) {
      convertToFollower(returnedTerm);
      return;
  }

  // Ignore if we're no longer leader
  if (serverState.load() != LEADER) {
    return;
  }

  // Ignore stale responses
  if (returnedTerm < currentTerm.load()) {
      return;
  }
  // Log_info("flag 11 - server %d: handleAppendResponse", loc_id_);

  if (success) {
    Log_info("APPEND SUCCESS: Server %d updating follower %d: matchIndex=%d, nextIndex=%d", 
      loc_id_, followerId, sentUpToIndex, sentUpToIndex + 1);
    matchIndex[followerId].store(sentUpToIndex);
    nextIndex[followerId].store(sentUpToIndex + 1);

    {
      std::lock_guard<std::mutex> lock(logs_mutex);
      for (int i = logs.size(); i > commitIndex.load() ; i-- ) {
        if(logs[i-1].first == currentTerm.load()) {
          int count = 1;
          for (int j = 0 ; j< SERVER_COUNT; j++) {
            if(j != loc_id_ && matchIndex[j].load() >= i) {
              count++;
            }
          }
          if (count > SERVER_COUNT/2) {
            Log_info("COMMIT UPDATE: Server %d committing index %d (count=%d, needed=%d)", 
              loc_id_, i, count, SERVER_COUNT/2 + 1);
            commitIndex.store(i);
            break;
          }
        }
      }
    }
    // Log_info("flag 13 - server %d: handleAppendResponse", loc_id_);

  } else {
    // Log_info("flag 14 - server %d: handleAppendResponse", loc_id_);

    Log_info("APPEND FAILED: Server %d retrying follower %d, decrementing nextIndex from %d to %d", 
      loc_id_, followerId, nextIndex[followerId].load(), nextIndex[followerId].load() - 1);
    nextIndex[followerId].fetch_sub(1);
    if (nextIndex[followerId].load() > 0) {
      int prevIndex = nextIndex[followerId].load() - 1;
      // CORRECT - Add mutex protection
      int prevTerm = 0;
      vector<shared_ptr<Marshallable>> entries;
      vector<uint64_t> entry_terms; 
      int retrySentUpToIndex;
      {
        std::lock_guard<std::mutex> lock(logs_mutex);
        if (prevIndex > 0 && prevIndex <= (int)logs.size()) {
            prevTerm = logs[prevIndex - 1].first;
        }
        // Send ALL entries from nextIndex onwards
        int currNextIdx = nextIndex[followerId].load();
        if (currNextIdx >= 1 && currNextIdx <= (int)logs.size()) {
            for (auto it = logs.begin() + (currNextIdx - 1); it != logs.end(); ++it) {
                entries.push_back(it->second);
                entry_terms.push_back(it->first);
            }
        }
        retrySentUpToIndex = currNextIdx - 1 + entries.size();
      }
      Log_info("RETRY APPEND: Server %d retrying append to follower %d, prevIndex=%d, prevTerm=%d, entries=%zu", 
        loc_id_, followerId, prevIndex, prevTerm, entries.size());
      commo() -> SendAppendEntries(0, followerId, currentTerm.load(), loc_id_, prevIndex, prevTerm, entries, entry_terms, commitIndex.load(), 
      [this, followerId, retrySentUpToIndex] (bool success, uint64_t returnedTerm, uint64_t follower_Id) {
        Log_info("RETRY RESPONSE: Server %d received retry response from follower %d: success=%d", 
          loc_id_, followerId, success);
        handleAppendResponse(success, returnedTerm, followerId, retrySentUpToIndex);
      });
    }
    // // Log_info("flag 15 - server %d: handleAppendResponse", loc_id_);

  }
  return;
}

void RaftServer::convertToFollower(uint64_t newTerm) {
  ServerState priorState = serverState.load();
  Log_info("STATE CHANGE: Server %d converting to FOLLOWER (was %d), newTerm=%d", 
    loc_id_, priorState, newTerm);
  //here we perform actions common to all prior server states
  currentTerm.store(newTerm);
  serverState.store(RaftServer::FOLLOWER);
  votedFor.store(-1);


  if (priorState == RaftServer::LEADER) {
    Log_info("LEADER STEPDOWN: Server %d (Leader) stepping down for new term %d", loc_id_, newTerm);
  } else if (priorState == RaftServer::CANDIDATE) {
    //clean up whatever was being used for the prior ongoing election we were running
    votesReceived.store(0);
    Log_info("CANDIDATE ABORT: Server %d (Candidate) aborting election for new term %d", loc_id_, newTerm);
  }


  resetElectionTimeout();
  lastHeartbeatTime.store(std::chrono::steady_clock::now());
}

void RaftServer::startElection() {

  //become a candidate
  serverState.store(RaftServer::CANDIDATE);
  
  //increment currentTerm
  currentTerm.fetch_add(1);

  Log_info("ELECTION START: Server %d starting election for term %d", loc_id_, currentTerm.load());

  //vote for self
  votedFor.store(loc_id_);
  votesReceived.store(1);

  //this is for our state management


  //reset election timer
  resetElectionTimeout();
  lastHeartbeatTime.store(std::chrono::steady_clock::now());

  
  //Send RequestVote RPCs to all other servers
  for (int i = 0 ; i < SERVER_COUNT ; i++) {
    if (i != loc_id_) {
      //now the handler will be inside this? but the handler is PER request, so how do we aggregate
      //and know the current tally (Twenty One Pilots reference???)
      
      //If AppendEntries RPC received from new leader: convert to follower - where do we do this?
      
      //If election timeout elapses: start new election - where do we do this?
      
      //sendRequestVote(partitionId, siteId, candidateTerm, candidateId, lastLogIndex, lastLogTerm) -> term, voteGranted
      int lastLogIndex, lastLogTerm;
      {
          std::lock_guard<std::mutex> lock(logs_mutex);
          lastLogIndex = logs.size(); //this makes it 0 for EMPTY case, actual size otherwise
          lastLogTerm = logs.empty() ? 0 : logs.back().first;
      }
      
      Log_info("SENDING VOTE REQUEST: Server %d sending vote request to server %d, term=%d, lastLogIndex=%d, lastLogTerm=%d", 
        loc_id_, i, currentTerm.load(), lastLogIndex, lastLogTerm);
      commo()->SendRequestVote(0, i, currentTerm.load(), loc_id_, lastLogIndex, lastLogTerm, 
      [this](bool voteGranted, uint64_t returnedTerm){
        Log_info("VOTE RESPONSE: Server %d received vote response: granted=%d, term=%d", loc_id_, voteGranted, returnedTerm);
        handleVoteResponse(voteGranted, returnedTerm);
      });
    }
  }
  


}

void RaftServer::resetElectionTimeout(){
  // Use a much wider range and server ID bias for better separation
  int randomDuration = 200 + (rand() % 201);          // 200 to 400 ms randomly here
  electionTimeout.store(std::chrono::milliseconds(randomDuration));
}


bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                       uint64_t *index,
                       uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  if (serverState.load() != RaftServer::LEADER) {
    return false;
  }

  //this command has just come from the client and I am the leader, so I can append to the log
  Log_info("NEW ENTRY: Server %d appending new entry to log, term=%d, logSize=%zu", 
    loc_id_, currentTerm.load(), logs.size());
  
  {
    std::lock_guard<std::mutex> lock(logs_mutex);
    logs.push_back({currentTerm.load(), cmd});
  }

  for (int i = 0; i < SERVER_COUNT ; i++ ){
    if (i != loc_id_) {
      vector<shared_ptr<Marshallable>> newEntries = {cmd};
      vector<uint64_t> newEntryTerms = {static_cast<uint64_t>(currentTerm.load())};
      // CORRECT - Add mutex protection
      uint64_t prevLogIndex, prevLogTerm;
      int sentUpToIndex;
      {
        std::lock_guard<std::mutex> lock(logs_mutex);
        prevLogIndex = logs.size() - 1;
        prevLogTerm = logs.size() > 1 ? logs[logs.size() - 2].first : 0;
        sentUpToIndex = logs.size();
      }
      Log_info("SENDING NEW ENTRY: Server %d sending new entry to follower %d, prevLogIndex=%d, prevLogTerm=%d", 
        loc_id_, i, prevLogIndex, prevLogTerm);
      commo() -> SendAppendEntries(0, i, currentTerm.load(), loc_id_,
        prevLogIndex, prevLogTerm, newEntries, newEntryTerms, commitIndex.load(), 
        [this, i, sentUpToIndex] (bool success, uint64_t returnedTerm, uint64_t followerId) {
          Log_info("NEW ENTRY RESPONSE: Server %d received new entry response from follower %d: success=%d", 
            loc_id_, i, success);
          handleAppendResponse(success, returnedTerm, i, sentUpToIndex);
        });
    }
  }

  {
    std::lock_guard<std::mutex> lock(logs_mutex);
    *index = logs.size();
  }
  *term = currentTerm.load();
  return true;
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  // Log_info("DEBUG: Server %d reports term=%ld, is_leader=%d", loc_id_, currentTerm.load(), *is_leader);
    *is_leader = (serverState.load() == RaftServer::LEADER);
  *term = currentTerm.load();

}

void RaftServer::SyncRpcExample() {
  /* This is an example of synchronous RPC using coroutine; feel free to 
     modify this function to dispatch/receive your own messages. 
     You can refer to the other function examples in commo.h/cc on how 
     to send/recv a Marshallable object over RPC. */
  Coroutine::CreateRun([this](){
    string res;
    auto event = commo()->SendString(0, /* partition id is always 0 for lab1 */
                                     0, "hello", &res);
    event->Wait(1000000); //timeout after 1000000us=1s
    if (event->status_ == Event::TIMEOUT) {
      // Log_info("timeout happens");
    } else {
      // Log_info("rpc response is: %s", res.c_str()); 
    }
  });
}

/* Do not modify any code below here */

void RaftServer::Disconnect(const bool disconnect) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(disconnected_ != disconnect);
  // global map of rpc_par_proxies_ values accessed by partition then by site
  static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
  if (_proxies.find(partition_id_) == _proxies.end()) {
    _proxies[partition_id_] = {};
  }
  RaftCommo *c = (RaftCommo*) commo();
  if (disconnect) {
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() > 0);
    auto sz = c->rpc_par_proxies_.size();
    _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
    c->rpc_par_proxies_ = {};
    verify(_proxies[partition_id_][loc_id_].size() == sz);
    verify(c->rpc_par_proxies_.size() == 0);
  } else {
    verify(_proxies[partition_id_][loc_id_].size() > 0);
    auto sz = _proxies[partition_id_][loc_id_].size();
    c->rpc_par_proxies_ = {};
    c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
    _proxies[partition_id_][loc_id_] = {};
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() == sz);
  }
  disconnected_ = disconnect;
}

bool RaftServer::IsDisconnected() {
  return disconnected_;
}

} // namespace janus
