package kv

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

	// Add any client-side state you want here
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	return kv
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	shardMap := kv.shardMap.GetState()
	nodes := shardMap.Nodes
	numShards := shardMap.NumShards
	if numShards <= 0 {
		return "", false, fmt.Errorf("no shards exist")
	}

	shardsToNodes := shardMap.ShardsToNodes
	if len(shardsToNodes) <= 0 {
		return "", false, fmt.Errorf("no shards exist")
	}

	hashedKey := GetShardForKey(key, numShards)
	shard := shardsToNodes[hashedKey]
	if len(shard) <= 0 {
		return "", false, fmt.Errorf("shard hosts no nodes") // return an error
	}
	randNodeIndex := rand.Intn(len(shard))
	index := randNodeIndex
	nodeName := shard[randNodeIndex]

	kvClient, err := kv.clientPool.GetClient(nodeName)

	for err != nil {
		logrus.Trace("get client returned nil with index ", index, " and shard len ", len(shard))
		index = (index + 1) % len(shard)
		if index == randNodeIndex {
			logrus.Errorf("failed to return client in kvClient")
			return "", false, fmt.Errorf("failed to return client in kvClient")
		}
		nodeName = shard[index]

		kvClient, err = kv.clientPool.GetClient(nodeName)
		if err != nil {
			continue
		}
	}

	req := &proto.GetRequest{
		Key: key,
	}

	response, err := kvClient.Get(ctx, req)

	for err != nil {
		logrus.Trace("kvClient.get returned nil with index ", index, " and shard len ", len(shard))
		index = (index + 1) % len(shard)
		if index == randNodeIndex {
			logrus.Errorf("failed to return client or value from kvClient")
			return "", false, fmt.Errorf("failed to return client or value from kvClient")
		}
		nodeName = shard[index]

		kvClient, err = kv.clientPool.GetClient(nodeName)
		if err != nil {
			continue
		}

		response, err = kvClient.Get(ctx, req)

		if err != nil {
			logrus.Trace("kvClient.get returned nil with index ", index, " and shard len ", len(shard))
			continue
		}
	}

	// if err != nil {
	// 	logrus.Error("kv was not found")
	// 	return "", false, err
	// }

	value := response.Value
	wasFound := response.WasFound

	logrus.Trace("client sending Get() request with key: ", key,
		" nodes: ", nodes, " numShards: ", numShards,
		" shardsToNodes: ", shardsToNodes[1], " hashedKey: ", hashedKey,
		" nodeNameTemp: ", nodeName, " responseValue: ", value,
		" responseWasFound: ", wasFound)

	return value, wasFound, nil
}

func (kv *Kv) GetAndSet(ctx context.Context, key string, value string, ttl time.Duration, nodeName string, result chan error) {
	kvClient, err := kv.clientPool.GetClient(nodeName)
	if err != nil {
		result <- err
	}
	_, err = kvClient.Set(ctx, &proto.SetRequest{Key: key, Value: value, TtlMs: int64(ttl / time.Millisecond)})

	result <- err
}

func (kv *Kv) DeleteAndSet(ctx context.Context, key string, nodeName string, result chan error) {
	kvClient, err := kv.clientPool.GetClient(nodeName)
	if err != nil {
		result <- err
	}
	_, err = kvClient.Delete(ctx, &proto.DeleteRequest{Key: key})

	result <- err
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	shardMap := kv.shardMap.GetState()
	numShards := shardMap.NumShards
	// fmt.Println(numShards)
	if numShards <= 0 {
		return fmt.Errorf("no shards exist")
	}

	shardsToNodes := shardMap.ShardsToNodes
	hashedKey := GetShardForKey(key, numShards)
	shard := shardsToNodes[hashedKey]

	if len(shard) <= 0 {
		return fmt.Errorf("no nodes in shards exist")
	}

	var errors error
	var finalError error
	finalError = nil

	c := make(map[int]chan error)

	for i := 0; i < len(shard); i++ {
		c[i] = make(chan error)
	}

	for i := 0; i < len(shard); i++ {
		nodeName := shard[i]
		// fmt.Println(nodeName)

		go kv.GetAndSet(ctx, key, value, ttl, nodeName, c[i])

	}

	for i := 0; i < len(shard); i++ {
		errors = <-c[i]

		if errors != nil {
			finalError = errors
		}
	}

	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	return finalError
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	shardMap := kv.shardMap.GetState()
	numShards := shardMap.NumShards
	if numShards <= 0 {
		return fmt.Errorf("no shards exist")
	}
	shardsToNodes := shardMap.ShardsToNodes
	hashedKey := GetShardForKey(key, numShards)
	shard := shardsToNodes[hashedKey]

	if len(shard) <= 0 {
		return fmt.Errorf("no nodes in shards exist")
	}

	var errors error
	var finalError error
	finalError = nil

	c := make(map[int]chan error)

	for i := 0; i < len(shard); i++ {
		c[i] = make(chan error)
	}

	for i := 0; i < len(shard); i++ {
		nodeName := shard[i]
		// fmt.Println(nodeName)

		go kv.DeleteAndSet(ctx, key, nodeName, c[i])
	}

	for i := 0; i < len(shard); i++ {
		errors = <-c[i]

		if errors != nil {
			finalError = errors
		}
	}

	return finalError
}
