package server_lib

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"

	"cs426.yale.edu/lab1/ranker"
	umc "cs426.yale.edu/lab1/user_service/mock_client"
	upb "cs426.yale.edu/lab1/user_service/proto"
	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vpb "cs426.yale.edu/lab1/video_service/proto"
	"google.golang.org/grpc/credentials/insecure"
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
	Options            VideoRecServiceOptions
	Stats              VideoRecServiceStats
	StatsChannels      VideoRecServiceStatsChannels
	userServiceConn    *grpc.ClientConn
	userServiceClient  UserServiceInterface
	videoServiceConn   *grpc.ClientConn
	videoServiceClient VideoServiceInterface
	trendingVideos     TrendingVideosCache
}

type VideoRecServiceStats struct {
	TotalRequests     uint64
	TotalErrors       uint64
	ActiveRequests    uint64
	TotalLatencyMs    uint64
	AverageLatencyMs  float32
	UserServerErrors  uint64
	VideoServerErrors uint64
}

type VideoRecServiceStatsChannels struct {
	TotalRequestsChannel     chan uint64
	TotalErrorsChannel       chan uint64
	ActiveRequestsChannel    chan int
	TotalLatencyMsChannel    chan uint64
	UserServerErrorsChannel  chan uint64
	VideoServerErrorsChannel chan uint64
	RequestStatsChannel      chan int
	ReceiveStatsChannel      chan VideoRecServiceStats
}

type TrendingVideosCache struct {
	sync.RWMutex
	videos []*vpb.VideoInfo
	timer  *time.Ticker
}

func MakeVideoRecServiceServer(options VideoRecServiceOptions) *VideoRecServiceServer {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	userConn, err := grpc.Dial(options.UserServiceAddr, opts...)
	if err != nil {
		// TODO: handle error
		fmt.Printf("grpc dial failed")
		return nil
	}
	// user service RPC client
	userClient := upb.NewUserServiceClient(userConn)

	videoConn, err := grpc.Dial(options.VideoServiceAddr, opts...)
	if err != nil {
		// TODO: handle error
		fmt.Printf("grpc dial for video service failed")
		return nil
	}
	// video service RPC client
	videoClient := vpb.NewVideoServiceClient(videoConn)

	statsChan := VideoRecServiceStatsChannels{
		TotalRequestsChannel:     make(chan uint64),
		TotalErrorsChannel:       make(chan uint64),
		ActiveRequestsChannel:    make(chan int),
		TotalLatencyMsChannel:    make(chan uint64),
		UserServerErrorsChannel:  make(chan uint64),
		VideoServerErrorsChannel: make(chan uint64),
		RequestStatsChannel:      make(chan int),
		ReceiveStatsChannel:      make(chan VideoRecServiceStats),
	}

	return &VideoRecServiceServer{
		Options:            options,
		Stats:              VideoRecServiceStats{},
		StatsChannels:      statsChan,
		userServiceConn:    userConn,
		userServiceClient:  userClient,
		videoServiceConn:   videoConn,
		videoServiceClient: videoClient,
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
		userServiceConn:    nil,
		userServiceClient:  mockUserServiceClient,
		videoServiceConn:   nil,
		videoServiceClient: mockVideoServiceClient,
	}
}

// func GetUserWithRetry(
// 	ctx context.Context,
// 	req *upb.GetUserRequest,
// 	backoff int,
// 	numRetries int,
// 	_ ...grpc.CallOption,
// ) (*upb.GetUserResponse, error) {

// 	var err error
// 	var resp interface{}
// 	for err != nil && numRetries > 0 {
// 		resp, err = fn(ctx, req)
// 		numRetries--
// 		fmt.Printf("%v", resp)
// 	}
// 	if err != nil { // if there is still an error, return error
// 		log.Printf("RPC call failed after %v retries. Fallback...\n", numRetries)
// 	}
// 	return nil, nil
// }

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

	server.StatsChannels.TotalRequestsChannel <- 1
	// increment active requests
	server.StatsChannels.ActiveRequestsChannel <- 1
	// time GetTopVideos()
	start := time.Now()
	resp, err := server._GetTopVideos(ctx, req)
	end := time.Now()
	server.StatsChannels.TotalLatencyMsChannel <- uint64(end.Sub(start).Milliseconds())
	// decrement active requests
	server.StatsChannels.ActiveRequestsChannel <- -1
	if err != nil {
		server.StatsChannels.TotalErrorsChannel <- 1
	}
	return resp, err
}

func (server *VideoRecServiceServer) _GetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {
	// get user info
	userResp, err := server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: []uint64{req.GetUserId()}})
	if err != nil {
		// TODO: handle error
		fmt.Printf("GetUser failed. Retrying...\n")
		server.StatsChannels.UserServerErrorsChannel <- 1
		userResp, err = server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: []uint64{req.GetUserId()}})
		if err != nil {
			fmt.Printf("GetUser failed twice.\n")
			return nil, err
		}
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
		subResp, err := server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: batch})
		if err != nil {
			// TODO: handle error
			fmt.Printf("GetUser failed. Retrying...\n")
			server.StatsChannels.UserServerErrorsChannel <- 1
			subResp, err = server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: batch})
			if err != nil {
				fmt.Printf("GetUser failed twice.\n")
				return nil, err
			}
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
		videoResp, err := server.videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{VideoIds: batch})
		if err != nil {
			// TODO: handle error
			fmt.Printf("GetVideo failed. Retrying...\n")
			server.StatsChannels.UserServerErrorsChannel <- 1
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
	server.StatsChannels.RequestStatsChannel <- 1
	stats := <-server.StatsChannels.ReceiveStatsChannel
	return &pb.GetStatsResponse{
		TotalRequests:      stats.TotalRequests,
		TotalErrors:        stats.TotalErrors,
		ActiveRequests:     stats.ActiveRequests,
		UserServiceErrors:  stats.UserServerErrors,
		VideoServiceErrors: stats.VideoServerErrors,
		AverageLatencyMs:   stats.AverageLatencyMs,
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
			server.StatsChannels.UserServerErrorsChannel <- 1
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
