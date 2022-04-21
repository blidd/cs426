package kvtest

import (
	"context"
	"testing"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

// Like previous labs, you must write some tests on your own.
// Add your test cases in this file and submit them as extra_test.go.
// You must add at least 5 test cases, though you can add as many as you like.
//
// You can use any type of test already used in this lab: server
// tests, client tests, or integration tests.
//
// You can also write unit tests of any utility functions you have in utils.go
//
// Tests are run from an external package, so you are testing the public API
// only. You can make methods public (e.g. utils) by making them Capitalized.

func TestBasicKVName(t *testing.T) {
	// Tests that our KV node can store a simple key and
	// value. Mainly for sanity, and helping to find
	// potential errors in our implementation.
	setup := MakeTestSetup(MakeBasicOneShard())
	err := setup.NodeSet("n1", "first_test", "first_value", 100*time.Second)
	assert.Nil(t, err)

	val, wasFound, err := setup.NodeGet("n1", "first_test")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Equal(t, "first_value", val)

	setup.Shutdown()
}

func TestShardAvailableAfterMappingChange(t *testing.T) {
	// Test that we can successfully change the shard
	// mapping, with our "deleted" shard still available
	setup := MakeTestSetup(MakeBasicOneShard())

	setup.UpdateShardMapping(map[int][]string{})
	numShards := setup.shardMap.NumShards()

	assert.Equal(t, 1, numShards)

	setup.Shutdown()
}

func TestRecoverShardMapping(t *testing.T) {
	// Test that we can successfully change the shard
	// mapping, with our "deleted" shard still available,
	// and then add it back successfully
	setup := MakeTestSetup(MakeBasicOneShard())

	setup.UpdateShardMapping(map[int][]string{})
	numShards := setup.shardMap.NumShards()
	assert.Equal(t, 1, numShards)

	_, _, err := setup.NodeGet("n1", "abc")
	assertShardNotAssigned(t, err)

	setup.UpdateShardMapping(map[int][]string{1: {"n1"}})
	err1 := setup.NodeSet("n1", "first_test", "first_value", 100*time.Second)
	assert.Nil(t, err1)

	val, wasFound, err2 := setup.NodeGet("n1", "first_test")
	assert.Nil(t, err2)
	assert.True(t, wasFound)
	assert.Equal(t, "first_value", val)

	setup.Shutdown()
}

func TestGetShardContentsBasic(t *testing.T) {

	setup := MakeTestSetup(MakeBasicOneShard())
	err := setup.NodeSet("n1", "abc", "123", 10*time.Second)
	assert.Nil(t, err)

	response, err := setup.nodes["n1"].GetShardContents(
		context.Background(),
		&proto.GetShardContentsRequest{Shard: 1},
	)
	assert.Nil(t, err)

	assert.Equal(t, response.Values[0].GetKey(), "abc")
	assert.Equal(t, response.Values[0].GetValue(), "123")
}

func TestGetShardContents(t *testing.T) {
	setup := MakeTestSetup(MakeTwoNodeMultiShard())

	_, err := setup.nodes["n1"].GetShardContents(
		context.Background(),
		&proto.GetShardContentsRequest{Shard: 6},
	)
	assertErrorWithCode(t, err, codes.NotFound)

	_, err = setup.nodes["n2"].GetShardContents(
		context.Background(),
		&proto.GetShardContentsRequest{Shard: 1},
	)
	assertErrorWithCode(t, err, codes.NotFound)

	setup.UpdateShardMapping(map[int][]string{
		1:  {"n2"},
		2:  {"n2"},
		3:  {"n2"},
		4:  {"n2"},
		5:  {"n2"},
		6:  {"n1"},
		7:  {"n1"},
		8:  {"n1"},
		9:  {"n1"},
		10: {"n1"},
	})

	_, err = setup.nodes["n1"].GetShardContents(
		context.Background(),
		&proto.GetShardContentsRequest{Shard: 1},
	)
	assertErrorWithCode(t, err, codes.NotFound)

	_, err = setup.nodes["n2"].GetShardContents(
		context.Background(),
		&proto.GetShardContentsRequest{Shard: 6},
	)
	assertErrorWithCode(t, err, codes.NotFound)
}
