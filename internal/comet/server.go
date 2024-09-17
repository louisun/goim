package comet

import (
	"context"
	"math/rand"
	"time"

	logic "github.com/Terry-Mao/goim/api/logic/grpc"
	"github.com/Terry-Mao/goim/internal/comet/conf"
	log "github.com/golang/glog"
	"github.com/zhenjl/cityhash"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/keepalive"
)

const (
	minServerHeartbeat = time.Minute * 10
	maxServerHeartbeat = time.Minute * 30
	// grpc options
	grpcInitialWindowSize     = 1 << 24
	grpcInitialConnWindowSize = 1 << 24
	grpcMaxSendMsgSize        = 1 << 24
	grpcMaxCallMsgSize        = 1 << 24
	grpcKeepAliveTime         = time.Second * 10
	grpcKeepAliveTimeout      = time.Second * 3
	grpcBackoffMaxDelay       = time.Second * 3
)

func newLogicClient(c *conf.RPCClient) logic.LogicClient {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.Dial))
	defer cancel()
	// 调用 logic 服务
	conn, err := grpc.DialContext(ctx, "discovery://default/goim.logic",
		[]grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallMsgSize)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(grpcMaxSendMsgSize)),
			grpc.WithBackoffMaxDelay(grpcBackoffMaxDelay),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                grpcKeepAliveTime,
				Timeout:             grpcKeepAliveTimeout,
				PermitWithoutStream: true,
			}),
			grpc.WithBalancerName(roundrobin.Name),
		}...)
	if err != nil {
		panic(err)
	}
	return logic.NewLogicClient(conn)
}

// Server is comet server.
type Server struct {
	// 配置信息
	config *conf.Config

	// Round Robin 策略获取的的共享资源
	round *Round

	// 分成 bucketSize 个 Bucket 桶来管理连接
	buckets    []*Bucket
	bucketSize uint32

	// 当前服务器 ID 标识
	serverID string

	// rpc 客户端
	rpcClient logic.LogicClient
}

// NewServer returns a new Server.
func NewServer(c *conf.Config) *Server {
	s := &Server{
		config:    c,
		round:     NewRound(c),
		rpcClient: newLogicClient(c.RPCClient),
	}

	// init bucket, 每个 bucket 都负责一个 map channel, rooms
	s.buckets = make([]*Bucket, c.Bucket.Size)
	s.bucketSize = uint32(c.Bucket.Size)

	for i := 0; i < c.Bucket.Size; i++ {
		// 创建 Bucket
		s.buckets[i] = NewBucket(c.Bucket)
	}

	s.serverID = c.Env.Host
	go s.onlineproc()
	return s
}

// Buckets return all buckets.
func (s *Server) Buckets() []*Bucket {
	return s.buckets
}

// Bucket get the bucket by subkey.
func (s *Server) Bucket(subKey string) *Bucket {
	idx := cityhash.CityHash32([]byte(subKey), uint32(len(subKey))) % s.bucketSize
	if conf.Conf.Debug {
		log.Infof("%s hit channel bucket index: %d use cityhash", subKey, idx)
	}
	return s.buckets[idx]
}

// RandServerHearbeat rand server heartbeat.
func (s *Server) RandServerHearbeat() time.Duration {
	return (minServerHeartbeat + time.Duration(rand.Int63n(int64(maxServerHeartbeat-minServerHeartbeat))))
}

// Close close the server.
func (s *Server) Close() (err error) {
	return
}

func (s *Server) onlineproc() {
	for {
		// 每 10s 执行
		var (
			allRoomsCount map[string]int32
			err           error
		)
		roomCount := make(map[string]int32)
		// 遍历 bucket 获取当前的 roomID 的在线人数
		for _, bucket := range s.buckets {
			for roomID, count := range bucket.RoomsCount() {
				roomCount[roomID] += count
			}
		}

		// 调用 logic rpc client 的 RenewOnline rpc  接口
		if allRoomsCount, err = s.RenewOnline(context.Background(), s.serverID, roomCount); err != nil {
			time.Sleep(time.Second)
			continue
		}

		// 更新每个 bucket AllOnline 的数量
		for _, bucket := range s.buckets {
			bucket.UpRoomsCount(allRoomsCount)
		}

		time.Sleep(time.Second * 10)
	}
}
