package server_lib

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"cs426.yale.edu/lab1/ranker"
	umc "cs426.yale.edu/lab1/user_service/mock_client"
	upb "cs426.yale.edu/lab1/user_service/proto"
	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vpb "cs426.yale.edu/lab1/video_service/proto"
)

type VideoRecServiceOptions struct {
	// Server address for the UserService"
	UserServiceAddr string
	// Server address for the VideoService
	VideoServiceAddr string
	// Maximum size of batches sent to UserService and VideoService
	MaxBatchSize int
	// If set, disable fallback to cache
	DisableFallback bool
	// If set, disable all retries
	DisableRetry bool
}

func DefaultVideoRecServiceOptions() VideoRecServiceOptions {
	return VideoRecServiceOptions{
		UserServiceAddr:  "[::1]:8081",
		VideoServiceAddr: "[::1]:8082",
		MaxBatchSize:     250,
	}
}

type UserServiceInterface interface {
	GetUser(
		ctx context.Context,
		req *upb.GetUserRequest,
		_ ...grpc.CallOption,
	) (*upb.GetUserResponse, error)
}

type VideoServiceInterface interface {
	GetVideo(
		ctx context.Context,
		req *vpb.GetVideoRequest,
		_ ...grpc.CallOption,
	) (*vpb.GetVideoResponse, error)
	GetTrendingVideos(
		ctx context.Context,
		req *vpb.GetTrendingVideosRequest,
		_ ...grpc.CallOption,
	) (*vpb.GetTrendingVideosResponse, error)
}

type VideoRecServiceServer struct {
	pb.UnimplementedVideoRecServiceServer
	Options VideoRecServiceOptions
	Stats   VideoRecServiceStats
	// userServiceConn    *grpc.ClientConn
	userServiceClient UserServiceInterface
	// videoServiceConn   *grpc.ClientConn
	videoServiceClient VideoServiceInterface
	trendingVideos     TrendingVideosCache
}

type VideoRecServiceStats struct {
	sync.RWMutex
	TotalRequests     uint64
	TotalErrors       uint64
	ActiveRequests    uint64
	TotalLatencyMs    uint64
	AverageLatencyMs  float32
	UserServerErrors  uint64
	VideoServerErrors uint64
}

type TrendingVideosCache struct {
	sync.RWMutex
	videos []*vpb.VideoInfo
	timer  *time.Ticker
}

func MakeVideoRecServiceServer(options VideoRecServiceOptions) *VideoRecServiceServer {

	return &VideoRecServiceServer{
		Options: options,
		Stats:   VideoRecServiceStats{TotalRequests: 1},
		trendingVideos: TrendingVideosCache{
			videos: make([]*vpb.VideoInfo, 0),
			timer:  time.NewTicker(time.Nanosecond),
		},
	}
}

func MakeVideoRecServiceServerWithMocks(
	options VideoRecServiceOptions,
	mockUserServiceClient *umc.MockUserServiceClient,
	mockVideoServiceClient *vmc.MockVideoServiceClient,
) *VideoRecServiceServer {
	// Implement your own logic here
	return &VideoRecServiceServer{
		Options:            options,
		userServiceClient:  mockUserServiceClient,
		videoServiceClient: mockVideoServiceClient,
	}
}

// wrap video info with rank
type RankedVideo struct {
	videoInfo *vpb.VideoInfo
	rank      uint64
}
type RankedVideoList []*RankedVideo

// enable sorting by rank
func (rvh RankedVideoList) Len() int           { return len(rvh) }
func (rvh RankedVideoList) Less(i, j int) bool { return rvh[i].rank > rvh[j].rank }
func (rvh RankedVideoList) Swap(i, j int)      { rvh[i], rvh[j] = rvh[j], rvh[i] }

func (server *VideoRecServiceServer) GetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	var userServiceClient upb.UserServiceClient
	var videoServiceClient vpb.VideoServiceClient
	if server.userServiceClient == nil && server.videoServiceClient == nil {
		userConn, err := grpc.Dial(server.Options.UserServiceAddr, opts...)
		if err != nil {
			// TODO: handle error
			fmt.Printf("grpc dial failed")
			return nil, err
		}
		defer userConn.Close()
		// user service RPC client
		userServiceClient = upb.NewUserServiceClient(userConn)

		videoConn, err := grpc.Dial(server.Options.VideoServiceAddr, opts...)
		if err != nil {
			// TODO: handle error
			fmt.Printf("grpc dial for video service failed")
			return nil, err
		}
		defer videoConn.Close()
		// video service RPC client
		videoServiceClient = vpb.NewVideoServiceClient(videoConn)
	}

	atomic.AddUint64(&server.Stats.TotalRequests, 1)
	atomic.AddUint64(&server.Stats.ActiveRequests, 1) // increment active requests

	start := time.Now()
	resp, err := server._GetTopVideos(ctx, req, userServiceClient, videoServiceClient)
	end := time.Now()
	atomic.AddUint64(&server.Stats.TotalLatencyMs, uint64(end.Sub(start).Milliseconds()))

	atomic.AddUint64(&server.Stats.ActiveRequests, ^uint64(0)) // decrement active requests
	if err != nil {
		atomic.AddUint64(&server.Stats.TotalErrors, 1)
	}

	return resp, err
}

func (server *VideoRecServiceServer) _GetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
	userServiceClient upb.UserServiceClient,
	videoServiceClient vpb.VideoServiceClient,
) (*pb.GetTopVideosResponse, error) {

	// get user info
	userResp, err := userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: []uint64{req.GetUserId()}})
	if err != nil {
		// TODO: handle error
		fmt.Printf("GetUser failed. Retrying... 1 %v\n", err)
		server.Stats.UserServerErrors++
		// userResp, err = server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: []uint64{req.GetUserId()}})
		// if err != nil {
		// 	fmt.Printf("GetUser failed twice.\n")
		// 	return nil, err
		// }
		return nil, err
	}
	subscribedTo := userResp.Users[0].GetSubscribedTo()

	// retrieve info of subscribed-to users
	userInfos := make([]*upb.UserInfo, 0)
	beg := 0
	for beg < len(subscribedTo) {
		// get the end of the batch slice
		end := beg + server.Options.MaxBatchSize
		if beg+server.Options.MaxBatchSize > len(subscribedTo) {
			end = len(subscribedTo)
		}
		batch := subscribedTo[beg:end]
		subResp, err := userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: batch})
		if err != nil {
			// TODO: handle error
			fmt.Printf("GetUser failed. Retrying... 2\n")
			server.Stats.UserServerErrors++
			// subResp, err = server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: batch})
			// if err != nil {
			// 	fmt.Printf("GetUser failed twice.\n")
			// 	return nil, err
			// }
			return nil, err
		}
		userInfos = append(userInfos, subResp.GetUsers()...)
		beg += server.Options.MaxBatchSize
	}
	// collect liked videos from each subscription and remove duplicates
	uniqueVideos := make(map[uint64]bool)
	for _, subscription := range userInfos {
		subVideos := subscription.GetLikedVideos()
		for _, id := range subVideos {
			if !uniqueVideos[id] {
				uniqueVideos[id] = true
			}
		}
	}
	videoIds := make([]uint64, len(uniqueVideos))
	i := 0
	for id := range uniqueVideos {
		videoIds[i] = id
		i++
	}

	videoInfos := make([]*vpb.VideoInfo, 0)
	// send requests in batches
	beg = 0
	for beg < len(videoIds) {
		// get the end of the batch slice
		end := beg + server.Options.MaxBatchSize
		if beg+server.Options.MaxBatchSize > len(videoIds) {
			end = len(videoIds)
		}
		batch := videoIds[beg:end]
		// get liked videos info
		videoResp, err := videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{VideoIds: batch})
		if err != nil {
			// TODO: handle error
			fmt.Printf("GetVideo failed. Retrying...\n")
			server.Stats.UserServerErrors++
			videoResp, err = server.videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{VideoIds: batch})
			if err != nil {
				fmt.Printf("GetVideo failed twice.\n")
				return nil, err
			}
		}

		videoInfos = append(videoInfos, videoResp.Videos...)
		beg += server.Options.MaxBatchSize
	}

	rankedVideoList := make(RankedVideoList, 0)
	var bcryptRanker ranker.BcryptRanker
	// calculate ranks for each liked video
	for _, video := range videoInfos {
		rank := bcryptRanker.Rank(
			userResp.Users[0].GetUserCoefficients(),
			video.GetVideoCoefficients(),
		)
		rankedVideoList = append(rankedVideoList, &RankedVideo{
			videoInfo: video,
			rank:      rank,
		})
	}
	// sort ranked videos
	sort.Sort(rankedVideoList)

	// deserialize into VideoInfo list to put in response
	videos := make([]*vpb.VideoInfo, 0)
	for _, video := range rankedVideoList {
		videos = append(videos, video.videoInfo)
	}
	// enforce limit
	if req.GetLimit() > 0 {
		videos = videos[:req.GetLimit()]
	}
	return &pb.GetTopVideosResponse{Videos: videos}, nil
}

func (server *VideoRecServiceServer) GetStats(
	ctx context.Context,
	req *pb.GetStatsRequest,
) (*pb.GetStatsResponse, error) {
	server.Stats.RLock()
	defer server.Stats.RUnlock()
	return &pb.GetStatsResponse{
		TotalRequests:      server.Stats.TotalRequests,
		TotalErrors:        server.Stats.TotalErrors,
		ActiveRequests:     server.Stats.ActiveRequests,
		UserServiceErrors:  server.Stats.UserServerErrors,
		VideoServiceErrors: server.Stats.VideoServerErrors,
		AverageLatencyMs:   float32(server.Stats.TotalLatencyMs / server.Stats.TotalRequests),
	}, nil
}

func (server *VideoRecServiceServer) GetTrendingVideosCache() []*vpb.VideoInfo {
	return server.trendingVideos.videos
}

func (server *VideoRecServiceServer) FetchTrendingVideos() error {
	resp, err := server.videoServiceClient.GetTrendingVideos(
		context.Background(),
		&vpb.GetTrendingVideosRequest{},
	)
	if err != nil {
		// TODO: handle error
		time.Sleep(10 * time.Second) // backoff 10 seconds

		return err
	}

	videoInfos := make([]*vpb.VideoInfo, 0)
	// send requests in batches
	beg := 0
	videoIds := resp.GetVideos()
	for beg < len(videoIds) {
		// get the end of the batch slice
		end := beg + server.Options.MaxBatchSize
		if beg+server.Options.MaxBatchSize > len(videoIds) {
			end = len(videoIds)
		}
		batch := videoIds[beg:end]
		// get liked videos info
		videoResp, err := server.videoServiceClient.GetVideo(context.Background(), &vpb.GetVideoRequest{VideoIds: batch})
		if err != nil {
			// TODO: handle error
			fmt.Printf("GetVideo failed. Retrying...\n")
			server.Stats.UserServerErrors++
			videoResp, err = server.videoServiceClient.GetVideo(context.Background(), &vpb.GetVideoRequest{VideoIds: batch})
			if err != nil {
				fmt.Printf("GetVideo failed twice.\n")
				return err
			}
		}
		videoInfos = append(videoInfos, videoResp.Videos...)
		beg += server.Options.MaxBatchSize
	}

	fmt.Printf("expiration time: %v\n", resp.ExpirationTimeS)

	server.trendingVideos.Lock()
	server.trendingVideos.videos = videoInfos     // cache videos
	server.trendingVideos.timer = time.NewTicker( // refresh timer to new value
		time.Duration(resp.ExpirationTimeS * uint64(time.Nanosecond)),
	)
	server.trendingVideos.Unlock()
	return nil
}

func (server *VideoRecServiceServer) GetTrendingVideosTimer() *time.Ticker {
	return server.trendingVideos.timer
}
