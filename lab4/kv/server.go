package kv

import (
	"context"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap    *ShardMap
	listener    *ShardMapListener
	clientPool  ClientPool
	shutdown    chan struct{}
	quitCleanup chan struct{}

	cache  map[int]*ShardCache
	shards []bool
}

type ShardCache struct {
	mu      *sync.RWMutex
	storage map[string]ShardState
}

type ShardState struct {
	value string
	ttl   time.Time
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
	shardsUpdate := make([]bool, server.shardMap.NumShards()+1)
	for _, shard := range server.shardMap.ShardsForNode(server.nodeName) {
		shardsUpdate[shard] = true
	}

	addSet := make([]int, 0)
	delSet := make([]int, 0)

	// fmt.Println("shards ", server.shards)
	for i := 1; i <= server.shardMap.NumShards(); i++ {
		if !server.shards[i] && shardsUpdate[i] {
			addSet = append(addSet, i)
		} else if server.shards[i] && !shardsUpdate[i] {
			delSet = append(delSet, i)
		}
	}

	for _, shard := range addSet {
		// add shards
		server.cache[shard] = &ShardCache{
			mu:      &sync.RWMutex{},
			storage: make(map[string]ShardState),
		}
	}
	for _, shard := range delSet {
		server.cache[shard] = &ShardCache{
			mu:      &sync.RWMutex{},
			storage: make(map[string]ShardState),
		}
	}
	copy(server.shards, shardsUpdate)
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			break
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()

	cache := make(map[int]*ShardCache)
	shards := make([]bool, shardMap.NumShards()+1)

	for _, shard := range shardMap.ShardsForNode(nodeName) {
		cache[shard] = &ShardCache{
			mu:      &sync.RWMutex{},
			storage: make(map[string]ShardState),
		}
		shards[shard] = true
	}

	server := KvServerImpl{
		nodeName:    nodeName,
		shardMap:    shardMap,
		listener:    &listener,
		clientPool:  clientPool,
		shutdown:    make(chan struct{}),
		quitCleanup: make(chan struct{}),
		cache:       cache,
		shards:      shards,
	}
	go server.shardMapListenLoop()
	server.handleShardMapUpdate()

	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				server.cleanupShardData()
			case <-server.quitCleanup:
				ticker.Stop()
				return
			}
		}
	}()

	return &server
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	server.quitCleanup <- struct{}{}
	server.listener.Close()
}

// asynchronous goroutine for cleaning up expired data
func (server *KvServerImpl) cleanupShardData() {
	for _, shard := range server.shardMap.ShardsForNode(server.nodeName) {
		for key, val := range server.cache[shard].storage {
			if time.Now().After(val.ttl) {
				server.cache[shard].mu.Lock()
				delete(server.cache[shard].storage, key)
				server.cache[shard].mu.Unlock()
			}
		}
	}
}

func (server *KvServerImpl) isShardHosted(shard int) bool {
	for _, sh := range server.shardMap.ShardsForNode(server.nodeName) {
		if sh == shard {
			return true
		}
	}
	return false
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	key := request.GetKey()
	if len(key) == 0 {
		return &proto.GetResponse{}, status.Error(codes.InvalidArgument, "invalid empty key")
	}

	shardNum := GetShardForKey(key, server.shardMap.NumShards())
	if !server.isShardHosted(shardNum) {
		return &proto.GetResponse{}, status.Error(codes.NotFound, "shard not hosted on this node")
	}

	server.cache[shardNum].mu.RLock()
	defer server.cache[shardNum].mu.RUnlock()
	state, ok := server.cache[shardNum].storage[key]
	if ok && time.Now().Before(state.ttl) {
		return &proto.GetResponse{
			Value:    state.value,
			WasFound: true,
		}, nil
	} else {
		return &proto.GetResponse{
			Value:    "",
			WasFound: false,
		}, nil
	}
}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	key := request.GetKey()
	val := request.GetValue()
	ttl := request.GetTtlMs()

	shard := GetShardForKey(key, server.shardMap.NumShards())
	if !server.isShardHosted(shard) {
		return &proto.SetResponse{}, status.Error(codes.NotFound, "shard not hosted on this node")
	}

	server.cache[shard].mu.Lock()
	defer server.cache[shard].mu.Unlock()
	server.cache[shard].storage[key] = ShardState{
		value: val,
		ttl:   time.Now().Add(time.Duration(ttl) * time.Millisecond),
	}

	return &proto.SetResponse{}, nil
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	key := request.GetKey()
	if len(key) == 0 {
		return &proto.DeleteResponse{}, status.Error(codes.InvalidArgument, "invalid empty key")
	}

	shard := GetShardForKey(key, server.shardMap.NumShards())
	if !server.isShardHosted(shard) {
		return &proto.DeleteResponse{}, status.Error(codes.NotFound, "shard not hosted on this node")
	}

	server.cache[shard].mu.Lock()
	defer server.cache[shard].mu.RUnlock()
	delete(server.cache[shard].storage, key)
	return &proto.DeleteResponse{}, nil
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {

	shardNum := int(request.GetShard())
	if !server.isShardHosted(shardNum) {
		return &proto.GetShardContentsResponse{}, status.Error(codes.NotFound, "shard not hosted on this node")
	}

	values := make([]*proto.GetShardValue, 0)
	for k, v := range server.cache[shardNum].storage {
		values = append(values, &proto.GetShardValue{
			Key:            k,
			Value:          v.value,
			TtlMsRemaining: time.Until(v.ttl).Milliseconds(),
		})
	}

	return &proto.GetShardContentsResponse{
		Values: values,
	}, nil
}
