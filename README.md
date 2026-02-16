# Raft Consensus in C++

[![C++](https://img.shields.io/badge/C%2B%2B-14-blue.svg)](https://en.cppreference.com/w/cpp/14)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Build](https://img.shields.io/badge/Build-Waf-orange.svg)](https://waf.io/)

A **Raft consensus protocol** implementation embedded in a distributed systems framework. Built as part of a distributed systems lab series (MIT 6.824–style) in C++.

---

## What This Is

This project is a **replicated state machine** built on the Raft consensus algorithm. It runs inside a transactional, sharded distributed systems framework and provides a fault-tolerant replicated log for a key-value store.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Distributed Key-Value Store                       │
├─────────────────────────────────────────────────────────────────────────┤
│  Client  ──►  Coordinator  ──►  RaftServer (Leader)  ──►  Log Replication │
│                                    │                                      │
│                    RequestVote / AppendEntries RPCs                        │
│                                    ▼                                      │
│              ┌─────────────────────────────────────────┐                  │
│              │  RaftServer (Followers)  ◄──►  RaftServer (Followers)      │
│              └─────────────────────────────────────────┘                  │
│                                    │                                      │
│                              Commit & Apply                               │
│                                    ▼                                      │
│                         In-Memory Key-Value Store                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## What I Built

I implemented the **Raft consensus layer** in `src/deptran/raft/`:

- **Leader election** — Candidates request votes; followers grant based on log freshness; randomized election timeouts to avoid split votes
- **Log replication** — Leader sends AppendEntries (heartbeats + log entries); followers accept or reject based on prevLogIndex/prevLogTerm
- **RPC handlers** — `RequestVote` and `AppendEntries` with correct term handling and log consistency checks
- **State machine** — Follower / Candidate / Leader state transitions with proper mutex protection
- **Commit propagation** — Leader advances commitIndex on quorum; all servers apply committed entries to the key-value store

The framework (RPC, scheduler, memdb, benchmarks) was provided; I focused on the Raft logic and integration.

---

## Features

| Feature | Status |
|---------|--------|
| Leader election with randomized timeouts | ✅ |
| Log replication (AppendEntries) | ✅ |
| Heartbeat-based liveness | ✅ |
| Log consistency (prevLogIndex/prevLogTerm) | ✅ |
| Commit index advancement (quorum) | ✅ |
| Re-election after leader failure | ✅ |
| Concurrent starts & backup tests | ✅ |
| 8 test cases passing | ✅ |

---

## Tech Stack

**C++14** · **Boost** (coroutines, threading) · **yaml-cpp** · **Waf** (build) · **Python** (test harness)

---

## Project Structure

| Path | Description |
|------|-------------|
| `src/deptran/raft/server.cc` | Raft state machine, election, log replication |
| `src/deptran/raft/service.cc` | RPC handlers (RequestVote, AppendEntries) |
| `src/deptran/raft/commo.cc` | RPC client (SendRequestVote, SendAppendEntries) |
| `src/deptran/raft/frame.cc` | Frame integration, executor/coordinator creation |
| `src/deptran/raft/coordinator.cc` | Client-side coordination |
| `src/deptran/raft/raft_rpc.rpc` | RPC definitions |
| `src/deptran/raft/test.cc` | Raft lab test suite |
| `src/rrr/` | RPC, reactor, coroutines |
| `src/memdb/` | In-memory key-value store |

---

## Highlights

| Area | Detail |
|------|--------|
| **Concurrency** | Mutexes for logs, state, vote/append responses; atomic term/votedFor |
| **Correctness** | Term comparison on every RPC; convert to follower on higher term |
| **Testing** | 8 tests: initial election, re-election, basic agree, fail agree, fail no agree, rejoin, concurrent starts, backup |
| **Integration** | Plugs into existing Frame/Communicator/Coordinator abstractions |

---

## Quick Start

**Prerequisites:** A modern Linux environment (e.g., Ubuntu 22.04) or equivalent. Use a virtual machine if needed.

**1. Clone and build**

```bash
git clone --recursive https://github.com/angad-singh97/dslabs-cpp-angad-singh97
cd dslabs-cpp-angad-singh97
./waf configure --protocol=raft
./waf build
```

**2. Install dependencies (Ubuntu 22.04)**

```bash
sudo apt-get update
sudo apt-get install -y \
    git pkg-config build-essential clang \
    libapr1-dev libaprutil1-dev libboost-all-dev \
    libyaml-cpp-dev libjemalloc-dev libgoogle-perftools-dev \
    python2 python3-dev python3-pip python3-wheel python3-setuptools
sudo pip3 install -r requirements.txt
```

**3. Run Raft tests**

```bash
./waf configure --protocol=raft --enable-raft-test
./waf build
./build/deptran_server -f config/raft_lab_test.yml
```

Or run the benchmark harness:

```bash
./build/deptran_server -f config/none_fpga_raft.yml -f config/1c1s3r1p.yml -f config/rw.yml -P localhost -d 10
python3 test_run.py
```

---

## License

MIT License. See [LICENSE](LICENSE).
