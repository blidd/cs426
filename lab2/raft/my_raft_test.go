package raft

import (
	"math/rand"
	"testing"
	"time"
)

func TestManyServersElections2A(t *testing.T) {

	servers := 11
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): many servers")

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
	cfg.disconnect((leader2 + 4) % servers)
	cfg.disconnect((leader2 + 5) % servers)
	time.Sleep(RaftElectionTimeout)
	cfg.checkNoLeader()

	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()
}

func TestManyServersReplication2B(t *testing.T) {
	servers := 21
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.one(101, servers, true)
	leader1 := cfg.checkOneLeader()

	cfg.disconnect(leader1)
	cfg.one(21, servers-1, true)
}

func TestRemoveUncommittedAndAgree2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 1) % servers)
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)

	time.Sleep(RaftElectionTimeout)

	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	cfg.disconnect(leader1)
	cfg.disconnect((leader1 + 4) % servers)

	cfg.connect((leader1 + 1) % servers)
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	leader2 := cfg.checkOneLeader()

	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	cfg.connect(leader1)
	cfg.connect((leader1 + 4) % servers)

	cfg.one(rand.Int(), servers, true)
}

func TestMultiplePartitions(t *testing.T) {

	servers := 9
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	leader1 := cfg.checkOneLeader()

	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)
	cfg.disconnect((leader1 + 5) % servers)

	cfg.disconnect((leader1 + 6) % servers)
	cfg.disconnect((leader1 + 7) % servers)
	cfg.disconnect((leader1 + 8) % servers)

	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)
	cfg.connect((leader1 + 5) % servers)

	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	cfg.disconnect(leader1)
	cfg.connect(leader1)

	cfg.disconnect(leader1)
	cfg.disconnect((leader1 + 1) % servers)
	cfg.disconnect((leader1 + 2) % servers)

	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	cfg.connect((leader1 + 6) % servers)
	cfg.connect((leader1 + 7) % servers)
	cfg.connect((leader1 + 8) % servers)

	// bring everyone back
	cfg.connect(leader1 % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)
	cfg.connect((leader1 + 5) % servers)

	cfg.one(rand.Int(), servers, false)
}

func TestTwoDisconnectedLeaders(t *testing.T) {

	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.one(100, 3, true)

	leader1 := cfg.checkOneLeader()
	for i := 0; i < 10; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}
	cfg.disconnect(leader1)

	leader2 := cfg.checkOneLeader()
	for i := 0; i < 10; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}
	cfg.disconnect(leader2)

	follower := 0
	if leader1+leader2 == 1 {
		follower = 2
	} else if leader1+leader2 == 2 {
		follower = 1
	}

	cfg.start1(follower, cfg.applier)
	cfg.connect(follower)

	cfg.connect(leader1)
	cfg.connect(leader2)

	time.Sleep(RaftElectionTimeout)

	cfg.one(rand.Int(), servers, true)

}
