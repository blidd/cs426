package main

import (
	"context"
	"testing"

	"hash/fnv"

	umc "cs426.yale.edu/lab1/user_service/mock_client"
	usl "cs426.yale.edu/lab1/user_service/server_lib"
	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	sl "cs426.yale.edu/lab1/video_rec_service/server_lib"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vsl "cs426.yale.edu/lab1/video_service/server_lib"
)

func buildMockGetTopVideosRequest() *pb.GetTopVideosRequest {
	// generate user id
	netId := "bl568"
	hasher := fnv.New64()
	hasher.Write([]byte(netId))

	// build mock request
	return &pb.GetTopVideosRequest{
		UserId: hasher.Sum64()%5000 + 200000,
		Limit:  10,
	}
}

// e2e test of GetTopVideos
func TestGetTopVideos(t *testing.T) {
	// build mock video recommendation service
	mockUserServiceClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	mockVideoServiceClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())

	vrs := sl.MakeVideoRecServiceServerWithMocks(
		sl.DefaultVideoRecServiceOptions(),
		mockUserServiceClient,
		mockVideoServiceClient,
	)

	req := buildMockGetTopVideosRequest()
	resp, err := vrs.GetTopVideos(context.Background(), req)
	if err != nil {
		t.Fatalf(`error occurred: %v\n`, err)
	} else if len(resp.Videos) == 0 {
		t.Fatalf(`no videos returned`)
	} else if len(resp.Videos) > 10 {
		t.Fatalf(`more than limit returned`)
	}
}

// batching
func TestBatching(t *testing.T) {
	// build mock video recommendation service
	mockUserServiceClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	mockVideoServiceClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())

	vrs := sl.MakeVideoRecServiceServerWithMocks(
		sl.VideoRecServiceOptions{
			UserServiceAddr:  "[::1]:8081",
			VideoServiceAddr: "[::1]:8082",
			MaxBatchSize:     5,
		},
		mockUserServiceClient,
		mockVideoServiceClient,
	)

	req := buildMockGetTopVideosRequest()
	// get top videos with batching
	resp, err := vrs.GetTopVideos(context.Background(), req)
	if err != nil {
		t.Fatalf(`error occurred: %v\n`, err)
	} else if len(resp.Videos) == 0 {
		t.Fatalf(`no videos returned`)
	} else if len(resp.Videos) > 10 {
		t.Fatalf(`more than limit returned`)
	}
}

// fallback
func TestRetryAndFallback(t *testing.T) {
	ctx := context.Background()

	// mockUserServiceClient fails
	mockUserServiceClient := umc.MakeMockUserServiceClient(usl.UserServiceOptions{
		FailureRate:  3,
		MaxBatchSize: 250,
	})
	mockVideoServiceClient := vmc.MakeMockVideoServiceClient(vsl.VideoServiceOptions{
		FailureRate:  3,
		MaxBatchSize: 250,
	})

	vrs := sl.MakeVideoRecServiceServerWithMocks(
		sl.DefaultVideoRecServiceOptions(),
		mockUserServiceClient,
		mockVideoServiceClient,
	)

	// errMsg := "User service unavailable, resorted to cached trending videos"
	req := buildMockGetTopVideosRequest()

	// the fallback should catch all errors when requests to the user or video service fails
	for i := 0; i < 20; i++ {
		_, err := vrs.GetTopVideos(ctx, req)
		if err != nil {
			t.Fatalf("Fallback should have caught error: %v", err)
		}
	}
}

// stats
func TestGetStats(t *testing.T) {
	ctx := context.Background()

	// mockUserServiceClient fails
	mockUserServiceClient := umc.MakeMockUserServiceClient(usl.UserServiceOptions{
		FailureRate:  3,
		MaxBatchSize: 250,
	})
	mockVideoServiceClient := vmc.MakeMockVideoServiceClient(vsl.VideoServiceOptions{
		FailureRate:  3,
		MaxBatchSize: 250,
	})

	vrs := sl.MakeVideoRecServiceServerWithMocks(
		sl.DefaultVideoRecServiceOptions(),
		mockUserServiceClient,
		mockVideoServiceClient,
	)

	// errMsg := "User service unavailable, resorted to cached trending videos"
	req := buildMockGetTopVideosRequest()

	// the fallback should catch all errors when requests to the user or video service fails
	for i := 0; i < 20; i++ {
		_, err := vrs.GetTopVideos(ctx, req)
		if err != nil {
			t.Fatalf("Fallback should have caught error: %v", err)
		}
	}

	stats, err := vrs.GetStats(ctx, &pb.GetStatsRequest{})
	if err != nil {
		t.Fatalf("Something went wrong while getting stats: %v", err)
	}
	if stats.TotalRequests != 20 {
		t.Fatalf("Should have 20 requests, not %d", stats.TotalRequests)
	}
	if stats.VideoServiceErrors == 0 {
		t.Fatalf("A video service error should have occurred")
	}
	if stats.UserServiceErrors == 0 {
		t.Fatalf("A user service error should have occurred")
	}
	if stats.AverageLatencyMs == 0 {
		t.Fatalf("Should have calculated an avg latency estimate")
	}
	if stats.ActiveRequests != 0 {
		t.Fatalf("Active requests should now be 0, not %d", stats.ActiveRequests)
	}
}

// fetch trending videos
func TestFetchTrendingVideos(t *testing.T) {
	// build mock video recommendation service
	mockUserServiceClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	mockVideoServiceClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())

	vrs := sl.MakeVideoRecServiceServerWithMocks(
		sl.DefaultVideoRecServiceOptions(),
		mockUserServiceClient,
		mockVideoServiceClient,
	)

	err := vrs.FetchTrendingVideos()
	if err != nil {
		t.Fatalf("fetch trending videos from video service failed: %v", err)
	}

	videos := vrs.GetTrendingVideosCache()
	if len(videos) == 0 {
		t.Fatalf("Trending videos cache empty")
	}
}

// test batching with retrys and fallbacks
func TestBatchingWithRetryAndFallbacks(t *testing.T) {
	ctx := context.Background()
	maxBatchSize := 10

	// mockUserServiceClient fails
	mockUserServiceClient := umc.MakeMockUserServiceClient(usl.UserServiceOptions{
		FailureRate:  10,
		MaxBatchSize: maxBatchSize,
	})
	mockVideoServiceClient := vmc.MakeMockVideoServiceClient(vsl.VideoServiceOptions{
		FailureRate:  10,
		MaxBatchSize: maxBatchSize,
	})

	vrs := sl.MakeVideoRecServiceServerWithMocks(
		sl.DefaultVideoRecServiceOptions(),
		mockUserServiceClient,
		mockVideoServiceClient,
	)
	vrs.Options.MaxBatchSize = maxBatchSize

	// just keep fetching until we get something in the cache
	for len(vrs.GetTrendingVideosCache()) == 0 {
		_ = vrs.FetchTrendingVideos()
	}

	req := buildMockGetTopVideosRequest()
	resp, err := vrs.GetTopVideos(ctx, req)
	if err != nil {
		t.Fatalf("GetTopVideos should not return error after fallbacks implemented: %v", err)
	}

	if len(resp.GetVideos()) == 0 {
		t.Fatalf("No videos returned")
	}
}
