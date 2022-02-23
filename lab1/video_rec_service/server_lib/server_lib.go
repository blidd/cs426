package server_lib

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

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
	Options                VideoRecServiceOptions
	Stats                  VideoRecServiceStats
	mockUserServiceClient  *umc.MockUserServiceClient
	mockVideoServiceClient *vmc.MockVideoServiceClient
	trendingVideos         TrendingVideos
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
	StaleResponses    uint64
}

type TrendingVideos struct {
	sync.RWMutex
	cache []*vpb.VideoInfo
	timer *time.Ticker
}

func MakeVideoRecServiceServer(options VideoRecServiceOptions) *VideoRecServiceServer {

	return &VideoRecServiceServer{
		Options: options,
		Stats:   VideoRecServiceStats{},
		trendingVideos: TrendingVideos{
			cache: make([]*vpb.VideoInfo, 0),
			timer: time.NewTicker(time.Nanosecond),
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
		Options:                options,
		mockUserServiceClient:  mockUserServiceClient,
		mockVideoServiceClient: mockVideoServiceClient,
	}
}

type clientConnClose func() error

func (server *VideoRecServiceServer) MakeUserServiceClient() (UserServiceInterface, clientConnClose, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if server.mockUserServiceClient == nil {
		conn, err := grpc.Dial(server.Options.UserServiceAddr, opts...)
		if err != nil {
			errStatus, _ := status.FromError(err)
			log.Printf("user grpc dial failed. Retrying...\n")
			log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
			atomic.AddUint64(&server.Stats.UserServerErrors, 1)
			atomic.AddUint64(&server.Stats.TotalErrors, 1)
			conn, err = grpc.Dial(server.Options.UserServiceAddr, opts...)
			if err != nil {
				errStatus, _ := status.FromError(err)
				log.Printf("user grpc dial failed twice\n")
				log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
				atomic.AddUint64(&server.Stats.UserServerErrors, 1)
				atomic.AddUint64(&server.Stats.TotalErrors, 1)
				return nil, nil, fmt.Errorf("failed to connect to user service: %w", err)
			}
		}
		// user service RPC client
		return upb.NewUserServiceClient(conn), conn.Close, nil
	} else {
		return server.mockUserServiceClient, nil, nil
	}
}

func (server *VideoRecServiceServer) MakeVideoServiceClient() (VideoServiceInterface, clientConnClose, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if server.mockVideoServiceClient == nil {
		conn, err := grpc.Dial(server.Options.VideoServiceAddr, opts...)
		if err != nil {
			errStatus, _ := status.FromError(err)
			log.Printf("video grpc dial failed. Retrying...\n")
			log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
			atomic.AddUint64(&server.Stats.VideoServerErrors, 1)
			atomic.AddUint64(&server.Stats.TotalErrors, 1)
			conn, err = grpc.Dial(server.Options.VideoServiceAddr, opts...)
			if err != nil {
				errStatus, _ := status.FromError(err)
				log.Printf("video grpc dial failed twice\n")
				log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
				atomic.AddUint64(&server.Stats.VideoServerErrors, 1)
				atomic.AddUint64(&server.Stats.TotalErrors, 1)
				return nil, nil, fmt.Errorf("failed to connect to video service: %w", err)
			}
		}
		// user service RPC client
		return vpb.NewVideoServiceClient(conn), conn.Close, nil
	} else {
		return server.mockVideoServiceClient, nil, nil
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

// GetTopVideos() RPC handler
func (server *VideoRecServiceServer) GetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {

	userServiceClient, usConnClose, err := server.MakeUserServiceClient()
	if err != nil {
		// return nil, fmt.Errorf("failed to create user service client: %w", err)
		return nil, status.Errorf(codes.Unavailable, "failed to create user service client: %w", err)
	}
	if usConnClose != nil {
		defer usConnClose()
	}
	videoServiceClient, vsConnClose, err := server.MakeVideoServiceClient()
	if err != nil {
		// return nil, fmt.Errorf("failed to create video service client: %w", err)
		return nil, status.Errorf(codes.Unavailable, "failed to create video service client: %w", err)
	}
	if vsConnClose != nil {
		defer vsConnClose()
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
	userServiceClient UserServiceInterface,
	videoServiceClient VideoServiceInterface,
) (*pb.GetTopVideosResponse, error) {

	// get user info
	userResp, err := userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: []uint64{req.GetUserId()}})
	if err != nil {
		errStatus, _ := status.FromError(err)
		log.Printf("GetUser failed. Retrying...\n")
		log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
		atomic.AddUint64(&server.Stats.UserServerErrors, 1)
		atomic.AddUint64(&server.Stats.TotalErrors, 1)
		userResp, err = userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: []uint64{req.GetUserId()}})
		if err != nil {
			errStatus, _ := status.FromError(err)
			log.Printf("GetUser failed twice. Fallback to trending videos...\n")
			log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
			atomic.AddUint64(&server.Stats.StaleResponses, 1)
			return &pb.GetTopVideosResponse{
				Videos:        server.GetTrendingVideosCache(),
				StaleResponse: true,
			}, nil
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
		subResp, err := userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: batch})
		if err != nil {
			errStatus, _ := status.FromError(err)
			log.Printf("GetUser failed. Retrying...\n")
			log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
			atomic.AddUint64(&server.Stats.UserServerErrors, 1)
			subResp, err = userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: batch})
			if err != nil {
				errStatus, _ := status.FromError(err)
				log.Printf("GetUser failed twice. Fallback to trending videos...\n")
				log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
				atomic.AddUint64(&server.Stats.StaleResponses, 1)
				return &pb.GetTopVideosResponse{
					Videos:        server.GetTrendingVideosCache(),
					StaleResponse: true,
				}, nil
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
		videoResp, err := videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{VideoIds: batch})
		if err != nil {
			errStatus, _ := status.FromError(err)
			log.Printf("GetVideo failed. Retrying...\n")
			log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
			atomic.AddUint64(&server.Stats.VideoServerErrors, 1)
			videoResp, err = videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{VideoIds: batch})
			if err != nil {
				errStatus, _ := status.FromError(err)
				log.Printf("GetVideo failed twice. Fallback to trending videos...\n")
				log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
				atomic.AddUint64(&server.Stats.StaleResponses, 1)
				return &pb.GetTopVideosResponse{
					Videos:        server.GetTrendingVideosCache(),
					StaleResponse: true,
				}, nil
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

	var avgLatencyMs float32
	if server.Stats.TotalRequests != 0 {
		avgLatencyMs = float32(server.Stats.TotalLatencyMs / server.Stats.TotalRequests)
	} else {
		avgLatencyMs = 0
	}

	return &pb.GetStatsResponse{
		TotalRequests:      server.Stats.TotalRequests,
		TotalErrors:        server.Stats.TotalErrors,
		ActiveRequests:     server.Stats.ActiveRequests,
		UserServiceErrors:  server.Stats.UserServerErrors,
		VideoServiceErrors: server.Stats.VideoServerErrors,
		AverageLatencyMs:   avgLatencyMs,
		StaleResponses:     server.Stats.StaleResponses,
	}, nil
}

func (server *VideoRecServiceServer) GetTrendingVideosCache() []*vpb.VideoInfo {
	server.trendingVideos.RLock()
	defer server.trendingVideos.RUnlock()
	return server.trendingVideos.cache
}

func (server *VideoRecServiceServer) GetTrendingVideosTimer() *time.Ticker {
	server.trendingVideos.RLock()
	defer server.trendingVideos.RUnlock()
	return server.trendingVideos.timer
}

func (server *VideoRecServiceServer) FetchTrendingVideos() error {
	videoServiceClient, vsConnClose, err := server.MakeVideoServiceClient()
	if err != nil {
		return fmt.Errorf("failed to create video service client: %w", err)
	}
	if vsConnClose != nil {
		defer vsConnClose()
	}

	resp, err := videoServiceClient.GetTrendingVideos(
		context.Background(),
		&vpb.GetTrendingVideosRequest{},
	)
	if err != nil {
		errStatus, _ := status.FromError(err)
		log.Printf("GetTrendingVideos failed. Retrying...\n")
		log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
		atomic.AddUint64(&server.Stats.VideoServerErrors, 1)
		resp, err = videoServiceClient.GetTrendingVideos(
			context.Background(),
			&vpb.GetTrendingVideosRequest{},
		)
		if err != nil {
			errStatus, _ := status.FromError(err)
			log.Printf("GetTrendingVideos failed twice. Canceling FetchTrendingVideos...\n")
			log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
			atomic.AddUint64(&server.Stats.VideoServerErrors, 1)
			return fmt.Errorf("failed to get trending videos: %w", err)
		}
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
		videoResp, err := videoServiceClient.GetVideo(context.Background(), &vpb.GetVideoRequest{VideoIds: batch})
		if err != nil {
			errStatus, _ := status.FromError(err)
			log.Printf("GetVideo failed. Retrying...\n")
			log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
			atomic.AddUint64(&server.Stats.VideoServerErrors, 1)
			videoResp, err = videoServiceClient.GetVideo(context.Background(), &vpb.GetVideoRequest{VideoIds: batch})
			if err != nil {
				errStatus, _ := status.FromError(err)
				log.Printf("GetVideo failed twice. Canceling FetchTrendingVideos...\n")
				log.Printf("(errcode %d) %v", errStatus.Code(), errStatus.Message())
				atomic.AddUint64(&server.Stats.VideoServerErrors, 1)
				return err
			}
		}
		videoInfos = append(videoInfos, videoResp.Videos...)
		beg += server.Options.MaxBatchSize
	}

	server.trendingVideos.Lock()
	server.trendingVideos.cache = videoInfos      // cache videos
	server.trendingVideos.timer = time.NewTicker( // refresh timer to new value
		time.Duration(resp.ExpirationTimeS * uint64(time.Nanosecond)),
	)
	server.trendingVideos.Unlock()
	return nil
}
