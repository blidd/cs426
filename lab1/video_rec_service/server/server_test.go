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

// e2e test of GetTOpVideos
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

// retry
func TestRetry(t *testing.T) {
	ctx := context.Background()

	// mockUserServiceClient fails
	mockUserServiceClient := umc.MakeMockUserServiceClient(usl.UserServiceOptions{
		FailureRate:  5,
		MaxBatchSize: 250,
	})
	mockVideoServiceClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())

	vrs := sl.MakeVideoRecServiceServerWithMocks(
		sl.DefaultVideoRecServiceOptions(),
		mockUserServiceClient,
		mockVideoServiceClient,
	)

	// errMsg := "User service unavailable, resorted to cached trending videos"
	req := buildMockGetTopVideosRequest()

	for i := 0; i < 20; i++ {
		_, err := vrs.GetTopVideos(ctx, req)
		if err != nil {
			t.Fatalf("Fallback should have caught error: %v", err)
		}
	}

	// get top videos with batching

	// if err != nil {
	// 	errStatus, _ := status.FromError(err)
	// 	// log.Printf("code %d        msg %v", errStatus.Code(), errStatus.Message())
	// 	// log.Printf("code %d        ", codes.Unavailable)
	// 	if errStatus.Code() != codes.Unavailable {
	// 		t.Fatalf("expecting Unavailable error code, but this occurred: %v\n", err)
	// 	}
	// 	if errStatus.Message() != errMsg {
	// 		t.Fatalf("unexpected error message returned: %v\n", err)
	// 	}
	// }
	// t.Fatalf("error should have occurred")
}

// stats

// error handling

// fallback
