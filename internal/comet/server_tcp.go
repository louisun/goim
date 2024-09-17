package comet

import (
	"context"
	"io"
	"net"
	"strings"
	"time"

	"github.com/Terry-Mao/goim/api/comet/grpc"
	"github.com/Terry-Mao/goim/internal/comet/conf"
	"github.com/Terry-Mao/goim/pkg/bufio"
	"github.com/Terry-Mao/goim/pkg/bytes"
	xtime "github.com/Terry-Mao/goim/pkg/time"
	log "github.com/golang/glog"
)

const (
	maxInt = 1<<31 - 1
)

// InitTCP listen all tcp.bind and start accept connections.
func InitTCP(server *Server, addrs []string, accept int) (err error) {
	var (
		bind     string
		listener *net.TCPListener
		addr     *net.TCPAddr
	)
	for _, bind = range addrs {
		if addr, err = net.ResolveTCPAddr("tcp", bind); err != nil {
			log.Errorf("net.ResolveTCPAddr(tcp, %s) error(%v)", bind, err)
			return
		}
		if listener, err = net.ListenTCP("tcp", addr); err != nil {
			log.Errorf("net.ListenTCP(tcp, %s) error(%v)", bind, err)
			return
		}
		log.Infof("start tcp listen: %s", bind)
		// split N core accept
		for i := 0; i < accept; i++ {
			go acceptTCP(server, listener)
		}
	}
	return
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.  Accept blocks; the caller typically
// invokes it in a go statement.
func acceptTCP(server *Server, lis *net.TCPListener) {
	var (
		conn *net.TCPConn
		err  error
		r    int
	)
	for {
		if conn, err = lis.AcceptTCP(); err != nil {
			// if listener close then return
			log.Errorf("listener.Accept(\"%s\") error(%v)", lis.Addr().String(), err)
			return
		}
		if err = conn.SetKeepAlive(server.config.TCP.KeepAlive); err != nil {
			log.Errorf("conn.SetKeepAlive() error(%v)", err)
			return
		}
		if err = conn.SetReadBuffer(server.config.TCP.Rcvbuf); err != nil {
			log.Errorf("conn.SetReadBuffer() error(%v)", err)
			return
		}
		if err = conn.SetWriteBuffer(server.config.TCP.Sndbuf); err != nil {
			log.Errorf("conn.SetWriteBuffer() error(%v)", err)
			return
		}
		go serveTCP(server, conn, r)
		if r++; r == maxInt {
			r = 0
		}
	}
}

func serveTCP(s *Server, conn *net.TCPConn, r int) {
	var (
		// timer: 自定义 Timer
		timer = s.round.Timer(r)
		// read bytes pool: *bytes.Pool
		readBytesPool = s.round.Reader(r)
		// write bytes pool: *bytes.Pool
		writeBytesPool = s.round.Writer(r)

		// ip addr
		lAddr = conn.LocalAddr().String()
		rAddr = conn.RemoteAddr().String()
	)
	if conf.Conf.Debug {
		log.Infof("start tcp serve \"%s\" with \"%s\"", lAddr, rAddr)
	}
	s.ServeTCP(conn, readBytesPool, writeBytesPool, timer)
}

// ServeTCP 处理一个 TCP 连接
func (s *Server) ServeTCP(conn *net.TCPConn, readPool, writePool *bytes.Pool, timer *xtime.Timer) {
	var (
		err error

		roomID            string           // 房间ID
		acceptProtocols   []int32          // 可接受的协议
		heartbeatInterval time.Duration    // 心跳间隔
		isInWhiteList     bool             // 是否在白名单中
		clientProto       *grpc.ProtoMsg   // 客户端协议消息
		bucket            *Bucket          // 连接的 bucket
		timerTask         *xtime.TimerTask // 定时器数据
		lastHeartbeat     = time.Now()     // 上次心跳时间

		readBuffer  = readPool.Get()  // 读取缓冲区
		writeBuffer = writePool.Get() // 写入缓冲区

		channel        = NewChannel(s.config.Protocol.MsgRingBufferSize, s.config.Protocol.MsgChannelSize) // 创建一个新的 Channel
		bufferedReader = &channel.Reader                                                                   // 缓冲读取器
		bufferedWriter = &channel.Writer                                                                   // 缓冲写入器
	)

	// 初始化读写缓冲区
	channel.Reader.ResetBuffer(conn, readBuffer.Bytes())
	channel.Writer.ResetBuffer(conn, writeBuffer.Bytes())

	// 创建上下文，方便后续取消
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 开始握手
	step := 0
	timerTask = timer.Add(time.Duration(s.config.Protocol.HandshakeTimeout), func() {
		conn.Close()
		log.Errorf("key: %s remoteIP: %s step: %d tcp handshake timeout", channel.Key, conn.RemoteAddr().String(), step)
	})

	// 获取客户端IP地址
	channel.IP, _, _ = net.SplitHostPort(conn.RemoteAddr().String())

	// 握手步骤1：读取客户端协议
	step = 1
	if clientProto, err = channel.MsgRing.GetWriteMsg(); err == nil {
		// 进行TCP认证
		if channel.MemberId, channel.Key, roomID, acceptProtocols, heartbeatInterval, err = s.authTCP(ctx, bufferedReader, bufferedWriter, clientProto); err == nil {
			// 认证成功后，设置监听的协议
			channel.Watch(acceptProtocols...)
			bucket = s.Bucket(channel.Key)
			err = bucket.AddChannel(roomID, channel)
			if conf.Conf.Debug {
				log.Infof("tcp connected key:%s mid:%d proto:%+v", channel.Key, channel.MemberId, clientProto)
			}
		}
	}

	// 握手步骤2：处理错误
	step = 2
	if err != nil {
		conn.Close()
		readPool.Put(readBuffer)
		writePool.Put(writeBuffer)
		timer.Remove(timerTask)
		log.Errorf("key: %s handshake failed error(%v)", channel.Key, err)
		return
	}

	// 设置心跳定时器
	timerTask.Key = channel.Key
	timer.Update(timerTask, heartbeatInterval)
	isInWhiteList = whitelist.Contains(channel.MemberId)
	if isInWhiteList {
		whitelist.Printf("key: %s[%s] auth\n", channel.Key, roomID)
	}

	// 握手步骤3：启动写协程
	go s.writePumpTcp(conn, bufferedWriter, writePool, writeBuffer, channel)

	// 随机生成服务器心跳间隔
	serverHeartbeatInterval := s.RandServerHearbeat()

	// 开始读写循环
	for {
		// 写入客户端协议
		if clientProto, err = channel.MsgRing.GetWriteMsg(); err != nil {
			break
		}

		if isInWhiteList {
			whitelist.Printf("key: %s start read proto\n", channel.Key)
		}

		// 读取TCP协议数据
		if err = clientProto.ReadTCP(bufferedReader); err != nil {
			break
		}

		if isInWhiteList {
			whitelist.Printf("key: %s read proto:%v\n", channel.Key, clientProto)
		}

		// 处理心跳协议
		if clientProto.Op == grpc.OpHeartbeat {
			timer.Update(timerTask, heartbeatInterval)
			clientProto.Op = grpc.OpHeartbeatReply
			clientProto.Body = nil

			// 发送服务器心跳
			if now := time.Now(); now.Sub(lastHeartbeat) > serverHeartbeatInterval {
				if err1 := s.Heartbeat(ctx, channel.MemberId, channel.Key); err1 == nil {
					lastHeartbeat = now
				}
			}

			if conf.Conf.Debug {
				log.Infof("tcp heartbeat receive key:%s, mid:%d", channel.Key, channel.MemberId)
			}

			step++
		} else {
			// 处理业务操作
			if err = s.Operate(ctx, clientProto, channel, bucket); err != nil {
				break
			}
		}

		if isInWhiteList {
			whitelist.Printf("key: %s process proto:%v\n", channel.Key, clientProto)
		}

		// 更新写索引并发信号
		channel.MsgRing.AdvanceWriteIndex()
		channel.ReadReady()

		if isInWhiteList {
			whitelist.Printf("key: %s msgChan\n", channel.Key)
		}
	}

	// 错误处理
	if isInWhiteList {
		whitelist.Printf("key: %s server tcp error(%v)\n", channel.Key, err)
	}

	// 处理非 EOF 错误
	if err != nil && err != io.EOF && !strings.Contains(err.Error(), "closed") {
		log.Errorf("key: %s server tcp failed error(%v)", channel.Key, err)
	}

	// 连接关闭处理
	bucket.RemoveChannel(channel)
	timer.Del(timerTask)
	readPool.Put(readBuffer)
	conn.Close()
	channel.SendFinishSignal()

	// 断开连接的回调
	if err = s.Disconnect(ctx, channel.MemberId, channel.Key); err != nil {
		log.Errorf("key: %s mid: %d operator do disconnect error(%v)", channel.Key, channel.MemberId, err)
	}

	if isInWhiteList {
		whitelist.Printf("key: %s mid: %d disconnect error(%v)\n", channel.Key, channel.MemberId, err)
	}

	if conf.Conf.Debug {
		log.Infof("tcp disconnected key: %s mid: %d", channel.Key, channel.MemberId)
	}
}

// dispatch accepts connections on the listener and serves requests
// for each incoming connection.  dispatch blocks; the caller typically
// invokes it in a go statement.
func (s *Server) writePumpTcp(conn *net.TCPConn, bufferedWriter *bufio.BufferedWriter, writeBytesPool *bytes.Pool, wb *bytes.Buffer, channel *Channel) {
	var (
		err    error
		finish bool
		online int32
		white  = whitelist.Contains(channel.MemberId)
	)
	if conf.Conf.Debug {
		log.Infof("key: %s start dispatch tcp goroutine", channel.Key)
	}
	for {
		if white {
			whitelist.Printf("key: %s wait proto ready\n", channel.Key)
		}
		var p = channel.Ready()
		if white {
			whitelist.Printf("key: %s proto ready\n", channel.Key)
		}
		if conf.Conf.Debug {
			log.Infof("key:%s dispatch msg:%v", channel.Key, *p)
		}
		switch p {
		case grpc.ProtoFinish:
			if white {
				whitelist.Printf("key: %s receive proto finish\n", channel.Key)
			}
			if conf.Conf.Debug {
				log.Infof("key: %s wakeup exit dispatch goroutine", channel.Key)
			}
			finish = true
			goto failed
		case grpc.ProtoReadReady:
			// fetch message from svrbox(client send)
			for {
				if p, err = channel.MsgRing.GetReadMsg(); err != nil {
					break
				}
				if white {
					whitelist.Printf("key: %s start write client proto%v\n", channel.Key, p)
				}
				if p.Op == grpc.OpHeartbeatReply {
					if channel.Room != nil {
						online = channel.Room.OnlineNum()
					}
					if err = p.WriteTCPHeart(bufferedWriter, online); err != nil {
						goto failed
					}
				} else {
					if err = p.WriteTCP(bufferedWriter); err != nil {
						goto failed
					}
				}
				if white {
					whitelist.Printf("key: %s write client proto%v\n", channel.Key, p)
				}
				p.Body = nil // avoid memory leak
				channel.MsgRing.AdvanceReadIndex()
			}
		default:
			if white {
				whitelist.Printf("key: %s start write server proto%v\n", channel.Key, p)
			}
			// server send
			if err = p.WriteTCP(bufferedWriter); err != nil {
				goto failed
			}
			if white {
				whitelist.Printf("key: %s write server proto%v\n", channel.Key, p)
			}
			if conf.Conf.Debug {
				log.Infof("tcp sent a message key:%s mid:%d proto:%+v", channel.Key, channel.MemberId, p)
			}
		}
		if white {
			whitelist.Printf("key: %s start flush \n", channel.Key)
		}
		// only hungry flush response
		if err = bufferedWriter.Flush(); err != nil {
			break
		}
		if white {
			whitelist.Printf("key: %s flush\n", channel.Key)
		}
	}
failed:
	if white {
		whitelist.Printf("key: %s dispatch tcp error(%v)\n", channel.Key, err)
	}
	if err != nil {
		log.Errorf("key: %s dispatch tcp error(%v)", channel.Key, err)
	}
	conn.Close()
	writeBytesPool.Put(wb)
	// must ensure all channel message discard, for reader won't blocking ReadReady
	for !finish {
		finish = (channel.Ready() == grpc.ProtoFinish)
	}
	if conf.Conf.Debug {
		log.Infof("key: %s dispatch goroutine exit", channel.Key)
	}
}

// auth for goim handshake with client, use rsa & aes.
func (s *Server) authTCP(ctx context.Context, rr *bufio.BufferedReader, wr *bufio.BufferedWriter, p *grpc.ProtoMsg) (mid int64, key, rid string, accepts []int32, hb time.Duration, err error) {
	for {
		if err = p.ReadTCP(rr); err != nil {
			return
		}
		if p.Op == grpc.OpAuth {
			break
		} else {
			log.Errorf("tcp request operation(%d) not auth", p.Op)
		}
	}
	if mid, key, rid, accepts, hb, err = s.Connect(ctx, p, ""); err != nil {
		log.Errorf("authTCP.Connect(key:%v).err(%v)", key, err)
		return
	}
	p.Op = grpc.OpAuthReply
	p.Body = nil
	if err = p.WriteTCP(wr); err != nil {
		log.Errorf("authTCP.WriteTCP(key:%v).err(%v)", key, err)
		return
	}
	err = wr.Flush()
	return
}
