package kv

import (
	"context"
	"fmt"
	"math/rand"
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

	cache      map[int]*ShardCache
	shards     []bool
	shardLocks []*sync.RWMutex
	mu         *sync.RWMutex
}

type ShardCache struct {
	storage map[string]ShardState
}

type ShardState struct {
	value string
	ttl   time.Time
}

// randomly select one of the nodes that hosts the shard
func (server *KvServerImpl) getNodeWithShard(shard int) string {
	nodeNames := server.shardMap.NodesForShard(shard)

	nodeNamesFiltered := make([]string, 0)
	for _, node := range nodeNames {
		if node != server.nodeName {
			nodeNamesFiltered = append(nodeNamesFiltered, node)
		}
	}

	if len(nodeNamesFiltered) > 0 {
		return nodeNamesFiltered[rand.Intn(len(nodeNamesFiltered))]
	} else {
		return ""
	}
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
	server.mu.Lock()
	defer server.mu.Unlock()
	shardsUpdate := make([]bool, server.shardMap.NumShards()+1)
	for _, shard := range server.shardMap.ShardsForNode(server.nodeName) {
		shardsUpdate[shard] = true
	}

	addSet := make([]int, 0)
	delSet := make([]int, 0)

	for i := 1; i <= server.shardMap.NumShards(); i++ {
		if !server.shards[i] && shardsUpdate[i] {
			addSet = append(addSet, i)
		} else if server.shards[i] && !shardsUpdate[i] {
			delSet = append(delSet, i)
		}
	}

	// several nodes host that shard data
	// figure out which nodes have that shard
	// use GetShardContents on that node
	// copy into your cache[shard]
	for _, shard := range addSet {
		// if the shard is in this node's addSet, that means the node
		// doesn't have the shard yet. So we have to remove the current
		// node from the list of nodeNames.

		// set up the about-to-be-added shard's data structure on our node
		// fmt.Printf("update lock: %d\n", shard)
		server.shardLocks[shard].Lock()
		server.cache[shard].storage = make(map[string]ShardState)
		server.shardLocks[shard].Unlock()

		// nodeName := server.getNodeWithShard(shard)
		// if len(nodeName) == 0 {
		// 	continue
		// }

		nodeNames := server.shardMap.NodesForShard(shard)

		nodeNamesFiltered := make([]string, 0)
		for _, node := range nodeNames {
			if node != server.nodeName {
				nodeNamesFiltered = append(nodeNamesFiltered, node)
			}
		}
		if len(nodeNamesFiltered) <= 0 {
			continue
		}

		index := rand.Intn(len(nodeNamesFiltered))
		randNodeIndex := index
		nodeName := nodeNamesFiltered[index]

		client, err := server.clientPool.GetClient(nodeName)
		breakLoop := false

		for err != nil {
			logrus.Trace("in server, get client returned nil with index ", index)
			index = (index + 1) % len(nodeNamesFiltered)
			if index == randNodeIndex {
				fmt.Errorf("failed to return client in server")
				breakLoop = true
				break
			}
			nodeName = nodeNamesFiltered[index]

			client, err = server.clientPool.GetClient(nodeName)
			if err != nil {
				continue
			}
		}

		if breakLoop {
			break
		}

		// get contents of shard from the selected node

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		response, err := client.GetShardContents(ctx, &proto.GetShardContentsRequest{Shard: int32(shard)})
		for err != nil {
			logrus.Trace("in server, get shard contents returned nil with shard ", shard, " and node name ", nodeName)
			index = (index + 1) % len(nodeNamesFiltered)
			if index == randNodeIndex {
				fmt.Errorf("failed to return client in server")
				breakLoop = true
				break
			}
			nodeName = nodeNamesFiltered[index]

			client, err = server.clientPool.GetClient(nodeName)
			if err != nil {
				continue
			}

			response, err = client.GetShardContents(ctx, &proto.GetShardContentsRequest{Shard: int32(shard)})
			if err != nil {
				continue
			}
		}

		if breakLoop {
			break
		}

		values := response.GetValues()
		for _, val := range values {
			server.shardLocks[shard].Lock()
			server.cache[shard].storage[val.Key] = ShardState{
				value: val.Value,
				ttl:   time.Now().Add(time.Duration(val.TtlMsRemaining) * time.Millisecond),
			}
			server.shardLocks[shard].Unlock()
		}
	}

	for _, shard := range delSet {
		server.cache[shard] = &ShardCache{
			// mu:      &sync.RWMutex{},
			storage: make(map[string]ShardState),
		}
	}

	for i := 1; i <= server.shardMap.NumShards(); i++ {
		server.shards[i] = shardsUpdate[i]
	}
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()

	cache := make(map[int]*ShardCache)
	shards := make([]bool, shardMap.NumShards()+1)
	shardLocks := make([]*sync.RWMutex, shardMap.NumShards()+1)
	for _, shard := range shardMap.ShardsForNode(nodeName) {
		// shards[shard] = true
		cache[shard] = &ShardCache{
			// mu:      &sync.RWMutex{},
			storage: make(map[string]ShardState),
		}
	}

	for i := 0; i < shardMap.NumShards()+1; i++ {
		shards[i] = false
		shardLocks[i] = &sync.RWMutex{}
	}

	for i := 0; i < shardMap.NumShards()+1; i++ {
		cache[i] = &ShardCache{
			// mu:      &sync.RWMutex{},
			storage: make(map[string]ShardState),
		}
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
		shardLocks:  shardLocks,
		mu:          &sync.RWMutex{},
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
		// if _, ok := server.cache[shard]; !ok {
		// 	continue
		// }
		server.shardLocks[shard].Lock()
		defer server.shardLocks[shard].Unlock()
		for key, val := range server.cache[shard].storage {
			if time.Now().After(val.ttl) {
				delete(server.cache[shard].storage, key)
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

	// if _, ok := server.cache[shardNum]; !ok {
	// 	return nil, status.Error(codes.NotFound, "shard not in cache in server get")
	// }

	server.shardLocks[shardNum].RLock()
	defer server.shardLocks[shardNum].RUnlock()
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
	if len(key) == 0 {
		return &proto.SetResponse{}, status.Error(codes.InvalidArgument, "invalid empty key")
	}
	val := request.GetValue()
	ttl := request.GetTtlMs()

	shard := GetShardForKey(key, server.shardMap.NumShards())
	if !server.isShardHosted(shard) {
		return &proto.SetResponse{}, status.Error(codes.NotFound, "shard not hosted on this node")
	}

	// if _, ok := server.cache[shard]; !ok {
	// 	return nil, status.Error(codes.NotFound, "shard not in cache in server set")
	// }

	server.shardLocks[shard].Lock()
	defer server.shardLocks[shard].Unlock()
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
	// if _, ok := server.cache[shard]; !ok {
	// 	return nil, status.Error(codes.NotFound, "shard not in cache in server delete")
	// }

	server.shardLocks[shard].RLock()
	defer server.shardLocks[shard].RUnlock()
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

	// if _, ok := server.cache[shardNum]; !ok {
	// 	return &proto.GetShardContentsResponse{}, status.Error(codes.NotFound, "shardNum not in cache")
	// }

	values := make([]*proto.GetShardValue, 0)

	// fmt.Printf("contents lock: %d\n", shardNum)
	server.shardLocks[shardNum].Lock()

	for k, v := range server.cache[shardNum].storage {
		values = append(values, &proto.GetShardValue{
			Key:            k,
			Value:          v.value,
			TtlMsRemaining: time.Until(v.ttl).Milliseconds(),
		})
	}

	server.shardLocks[shardNum].Unlock()

	return &proto.GetShardContentsResponse{
		Values: values,
	}, nil
}
