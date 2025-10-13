

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
  lastHeartbeatTime = std::chrono::steady_clock::now() + std::chrono::milliseconds(1000 + (loc_id_ * 200));
  resetElectionTimeout();
  
  // SyncRpcExample();
  Coroutine::CreateRun([this]() {
    while (true) {
      auto now = std::chrono::steady_clock::now();

      if ((serverState == RaftServer::FOLLOWER || serverState == RaftServer::CANDIDATE) && now >= lastHeartbeatTime + electionTimeout) {
        Log_info("Starting election as %s - timeout expired - SERVER %d", 
          (serverState == RaftServer::FOLLOWER) ? "FOLLOWER" : "CANDIDATE", loc_id_);
        startElection();
        resetElectionTimeout();
      }

      Coroutine::Sleep(100);
    }

  });

  Coroutine::CreateRun([this]() {
    while (true) {
      if (serverState == RaftServer::LEADER) {
        for (int i = 0 ; i < SERVER_COUNT ; i++) {
          if (i == loc_id_) continue;

          int currNextIndex = nextIndex[i];
          int prevIdx = currNextIndex - 1;
          uint64_t prevTerm = prevIdx > 0? logs[prevIdx - 1].term : 0;

          vector<LogStruct> entries;

          if (currNextIndex >= 1 && currNextIndex <= (int)logs.size()) {
            entries.insert(entries.end(), logs.begin() + (currNextIndex - 1), logs.end());
          }

          commo() -> SendAppendEntries(0, i, currentTerm, loc_id_, prevIdx, prevTerm, entries, commitIndex, this);
        } 
      }
      Coroutine::Sleep(100);
    }

  });


  /*Coroutine::CreateRun([this]() {
    while (true) {
      if (serverState == RaftServer::LEADER) {
        Log_info("Leader sending heartbeats");

        for (int i = 0 ; i < SERVER_COUNT ; i++) {
          if (i != loc_id_) {
            vector<LogStruct> emptyEntries;
            commo() -> SendAppendEntries(0, i, currentTerm, loc_id_, logs.size(), 
            logs.empty() ? 0 : logs.back().term, emptyEntries, commitIndex, this);
          }
        }
      }
  

      Coroutine::Sleep(100);
    }

  });*/

  Coroutine::CreateRun([this]() {
    while (true) {
      // while (lastApplied < commitIndex && lastApplied < (int)logs.size()) {
      //   lastApplied++;
      //   // app_next_ callback not implemented yet
      //   // if (app_next_) {
      //   //   app_next_(*logs[lastApplied-1].command);
      //   // }
      //   Log_info("Applied entry %d: term=%d", lastApplied, logs[lastApplied-1].term);
      // }
      // Coroutine::Sleep(50); 
    }
  });
}

void RaftServer::handleVoteResponse (bool voteGranted, uint64_t returnedTerm) {



  //Universal-term check, the paper says we do this for ANY request/response RPC received
  if (returnedTerm >  currentTerm) {
    convertToFollower(returnedTerm);
    Log_info("Converted to follower due to higher term in vote response. returnedTerm=%lu, currentTerm=%d", returnedTerm, currentTerm);
    return;
  }

  if (serverState != CANDIDATE){
    Log_info("Ignoring late vote response: server %d is now %s (was candidate)", 
             loc_id_, (serverState == LEADER) ? "LEADER" : "FOLLOWER");
    return;
  }

  if (returnedTerm < currentTerm) {
    Log_info("handleVoteResponse: Received vote response with older term (returnedTerm=%lu < currentTerm=%d), no action taken. voteGranted=%d, serverState=%d, votesReceived=%d",
             returnedTerm, currentTerm, voteGranted, serverState, votesReceived);
    return;
  }

  if (voteGranted) {
    votesReceived++;
    Log_info("Received voteGranted=true. votesReceived=%d, currentTerm=%d, serverState=%d", votesReceived, currentTerm, serverState);
    int majority = (SERVER_COUNT/2) + 1;
    if (votesReceived >= majority) {
      Log_info("LEADER TRANSITION: Server %d is now LEADER for term %lu", loc_id_, currentTerm);
      serverState = RaftServer::LEADER;
      votesReceived = 0; // Reset vote counter after becoming leader

      int nextLogIndex = logs.size() + 1;
      for (int i = 0; i < SERVER_COUNT ; i++){
        nextIndex[i] = nextLogIndex;
        matchIndex[i] = 0;
      }

      return;
    }
  }
}

void RaftServer::handleAppendResponse(bool success, uint64_t returnedTerm, int followerId) {
  if (returnedTerm != currentTerm) {
    return;
  }
  if (success) {
    int lastEntryIndex = logs.size();
    matchIndex[followerId] = lastEntryIndex;
    nextIndex[followerId] = lastEntryIndex + 1;

    for (int i = logs.size(); i > commitIndex ; i-- ){
       if(logs[i-1].term == currentTerm) {
        int count = 1;
        for (int j = 0 ; j< SERVER_COUNT; j++) {
          if(j != loc_id_ && matchIndex[j] >= i) {
            count++;
          }
        }
        if (count > SERVER_COUNT/2) {
          commitIndex = i;
          break;
        }
      
      }
    }


  } else {
    nextIndex[followerId]--;
    if (nextIndex[followerId] > 0) {
      int prevIndex = nextIndex[followerId] - 1;
      int prevTerm = (prevIndex > 0) ? logs[prevIndex - 1].term : 0;
      vector<LogStruct> retryEntry = {logs[prevIndex]};
      commo() -> SendAppendEntries(0, followerId, currentTerm, loc_id_, prevIndex, prevTerm, retryEntry, commitIndex, this);
    }
  }
}

void RaftServer::convertToFollower(uint64_t newTerm) {
  ServerState priorState = serverState;

  //here we perform actions common to all prior server states
  currentTerm = newTerm;
  serverState = RaftServer::FOLLOWER;
  votedFor = -1;


  if (priorState == RaftServer::LEADER) {
    Log_info("Server %d (Leader) stepping down for new term %lu", loc_id_, newTerm);
  } else if (priorState == RaftServer::CANDIDATE) {
    //clean up whatever was being used for the prior ongoing election we were running
    votesReceived = 0;
    Log_info("Server %d (Candidate) aborting election for new term %lu", loc_id_, newTerm);
  }


  resetElectionTimeout();
  lastHeartbeatTime = std::chrono::steady_clock::now();
}

void RaftServer::startElection() {

  //become a candidate
  serverState = RaftServer::CANDIDATE;
  
  //increment currentTerm
  currentTerm++;


  //vote for self
  votedFor = loc_id_;
  votesReceived = 1;//I will always vote for myself

  //this is for our state management


  //reset election timer
  resetElectionTimeout();
  lastHeartbeatTime = std::chrono::steady_clock::now();

  
  //Send RequestVote RPCs to all other servers
  for (int i = 0 ; i < SERVER_COUNT ; i++) {
    if (i != loc_id_) {
      //now the handler will be inside this? but the handler is PER request, so how do we aggregate
      //and know the current tally (Twenty One Pilots reference???)
      
      //If AppendEntries RPC received from new leader: convert to follower - where do we do this?
      
      //If election timeout elapses: start new election - where do we do this?
      
      //sendRequestVote(partitionId, siteId, candidateTerm, candidateId, lastLogIndex, lastLogTerm) -> term, voteGranted
      int lastLogIndex = logs.size(); //this makes it 0 for EMPTY case, actual size otherwise
      int lastLogTerm = logs.empty() ? 0 : logs.back().term;
      
      Log_info("Server %d sending RequestVote to server %d: term=%lu, lastLogIndex=%d, lastLogTerm=%d", 
                loc_id_, i, currentTerm, lastLogIndex, lastLogTerm);
      commo()->SendRequestVote(0, i, currentTerm, loc_id_, lastLogIndex, lastLogTerm, this);
    }
  }
  


}

void RaftServer::resetElectionTimeout(){
  // Use a much wider range and server ID bias for better separation
  int randomDuration = 200 + (rand() % 201);          // 200 to 400 ms randomly here
  electionTimeout = std::chrono::milliseconds(randomDuration);
}


bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                       uint64_t *index,
                       uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  if (serverState != RaftServer::LEADER) {
    return false;
  }

  //this command has just come from the client and I am the leader, so I can append to the log
  LogStruct newEntry;
  newEntry.term = currentTerm;
  newEntry.command = cmd;
  logs.push_back(newEntry);

  /*for (int i = 0; i < SERVER_COUNT ; i++ ){
    if (i != loc_id_) {
      vector<LogStruct> newEntries = {newEntry};
      commo() -> SendAppendEntries(0, i, currentTerm, loc_id_,
        logs.size() - 1, logs.size() > 1 ? logs[logs.size() - 2].term : 0, newEntries, commitIndex, this);
    }
  }*/

  *index = logs.size();
  *term = currentTerm;
  return true;
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  Log_info("DEBUG: Server %d reports term=%ld, is_leader=%d", loc_id_, currentTerm, *is_leader);
  *is_leader = (serverState == RaftServer::LEADER);
  *term = currentTerm;

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
      Log_info("timeout happens");
    } else {
      Log_info("rpc response is: %s", res.c_str()); 
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
