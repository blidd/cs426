package raft

import (
	"testing"
	"time"
)

func TestManyServersElection(t *testing.T) {

	servers := 7
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()

	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	time.Sleep(RaftElectionTimeout)

	// if the old leader rejoins, that shouldn't
	// disturb the new leader.
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()
	if leader1 == leader2 {
		t.Fatalf("old leader should become follower when rejoining")
	}

	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	cfg.disconnect((leader2 + 2) % servers)
	cfg.disconnect((leader2 + 3) % servers)
	time.Sleep(RaftElectionTimeout)
	cfg.checkNoLeader()

	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()
}
