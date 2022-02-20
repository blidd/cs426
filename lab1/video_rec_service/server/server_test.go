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

// TestHelloName calls greetings.Hello with a name, checking
// for a valid return value.
func TestGetVideos(t *testing.T) {

	// build mock video recommendation service
	mockUserServiceClient := umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	mockVideoServiceClient := vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrs := sl.MakeVideoRecServiceServerWithMocks(
		sl.DefaultVideoRecServiceOptions(),
		mockUserServiceClient,
		mockVideoServiceClient,
	)

	// generate user id
	netId := "bl568"
	hasher := fnv.New64()
	hasher.Write([]byte(netId))

	// build mock request
	req := &pb.GetTopVideosRequest{
		UserId: hasher.Sum64()%5000 + 200000,
		Limit:  10,
	}

	resp, err := vrs.GetTopVideos(context.Background(), req)
	if err != nil {
		t.Fatalf(`error occurred: %v\n`, err)
	} else if len(resp.Videos) == 0 {
		t.Fatalf(`no videos returned`)
	} else if len(resp.Videos) > 10 {
		t.Fatalf(`more than limit returned`)
	}
}
