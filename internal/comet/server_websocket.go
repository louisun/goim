package comet

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"strings"
	"time"

	"github.com/Terry-Mao/goim/api/comet/grpc"
	"github.com/Terry-Mao/goim/internal/comet/conf"
	"github.com/Terry-Mao/goim/pkg/bytes"
	xtime "github.com/Terry-Mao/goim/pkg/time"
	"github.com/Terry-Mao/goim/pkg/websocket"
	log "github.com/golang/glog"
)

// InitWebsocket listen all tcp.bind and start accept connections.
func InitWebsocket(server *Server, addrs []string, accept int) (err error) {
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
		// 直接 tcp listener 监听，没使用 http server
		if listener, err = net.ListenTCP("tcp", addr); err != nil {
			log.Errorf("net.ListenTCP(tcp, %s) error(%v)", bind, err)
			return
		}
		log.Infof("start ws listen: %s", bind)
		// split N core accept
		for i := 0; i < accept; i++ {
			go acceptWebsocket(server, listener)
		}
	}
	return
}

// InitWebsocketWithTLS init websocket with tls.
func InitWebsocketWithTLS(server *Server, addrs []string, certFile, privateFile string, accept int) (err error) {
	var (
		bind     string
		listener net.Listener
		cert     tls.Certificate
		certs    []tls.Certificate
	)
	certFiles := strings.Split(certFile, ",")
	privateFiles := strings.Split(privateFile, ",")
	for i := range certFiles {
		cert, err = tls.LoadX509KeyPair(certFiles[i], privateFiles[i])
		if err != nil {
			log.Errorf("Error loading certificate. error(%v)", err)
			return
		}
		certs = append(certs, cert)
	}
	tlsCfg := &tls.Config{Certificates: certs}
	tlsCfg.BuildNameToCertificate()
	for _, bind = range addrs {
		if listener, err = tls.Listen("tcp", bind, tlsCfg); err != nil {
			log.Errorf("net.ListenTCP(tcp, %s) error(%v)", bind, err)
			return
		}
		log.Infof("start wss listen: %s", bind)
		// split N core accept
		for i := 0; i < accept; i++ {
			go acceptWebsocketWithTLS(server, listener)
		}
	}
	return
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.  Accept blocks; the caller typically
// invokes it in a go statement.
func acceptWebsocket(server *Server, lis *net.TCPListener) {
	var (
		conn *net.TCPConn
		err  error
		r    int
	)
	for {
		if conn, err = lis.AcceptTCP(); err != nil {
			// if listener close then return
			log.Errorf("listener.Accept(%s) error(%v)", lis.Addr().String(), err)
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
		go serveWebsocket(server, conn, r)
		if r++; r == maxInt {
			r = 0
		}
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.  Accept blocks; the caller typically
// invokes it in a go statement.
func acceptWebsocketWithTLS(server *Server, lis net.Listener) {
	var (
		conn net.Conn
		err  error
		r    int
	)
	for {
		if conn, err = lis.Accept(); err != nil {
			// if listener close then return
			log.Errorf("listener.Accept(\"%s\") error(%v)", lis.Addr().String(), err)
			return
		}
		go serveWebsocket(server, conn, r)
		if r++; r == maxInt {
			r = 0
		}
	}
}

func serveWebsocket(s *Server, conn net.Conn, r int) {
	var (
		// timer: 自定义 Timer
		timer = s.round.Timer(r)
		// read bytes pool: *bytes.Pool
		readBytesPool = s.round.Reader(r)
		// write bytes pool: *bytes.Pool
		writeBytesPool = s.round.Writer(r)
	)
	if conf.Conf.Debug {
		// ip addr
		lAddr := conn.LocalAddr().String()
		rAddr := conn.RemoteAddr().String()
		log.Infof("start tcp serve \"%s\" with \"%s\"", lAddr, rAddr)
	}
	s.ServeWebsocket(conn, readBytesPool, writeBytesPool, timer)
}

// ServeWebsocket 处理一个 WebSocket 连接
func (s *Server) ServeWebsocket(conn net.Conn, readPool, writePool *bytes.Pool, timer *xtime.Timer) {
	var (
		err               error
		roomID            string         // 房间ID
		acceptOps         []int32        // 可接受的协议
		heartbeatInterval time.Duration  // 心跳间隔
		isInWhiteList     bool           // 是否在白名单中
		msg               *grpc.ProtoMsg // 客户端协议消息
		bucket            *Bucket        // 连接的 bucket

		timerTask     *xtime.TimerTask // 定时器数据
		lastHeartbeat = time.Now()     // 上次心跳时间

		// 从 Pool 拿到一块 Buffer，底层的 []byte 可供 BufferedReader 读取使用
		readBuffer = readPool.Get()

		// 创建一个新的 Channel，底层 MsgRingBuffer 长度默认为 5，写 channel 大小默认为 10
		channel = NewChannel(s.config.Protocol.MsgRingBufferSize, s.config.Protocol.MsgChannelSize)

		bufferedReader = &channel.Reader // 缓冲读取器指针，后面 channel.Reader.ResetBuffer() 进行初始化
		bufferedWriter = &channel.Writer // 缓冲写入器指针，后面 channel.Writer.ResetBuffer() 进行初始化

		wsConn    *websocket.Conn    // WebSocket 连接（封装 conn 和 bufferedReader，bufferedWriter 方便读写数据）
		wsRequest *websocket.Request // 暂存当前 WebSocket 请求
	)

	// 初始化 BufferedReader，底层 io.Reader 是 conn，读取的数组为 readBuffer 的 []byte（默认 8KB 大小）
	channel.Reader.ResetBuffer(conn, readBuffer.Bytes())

	// 初始化 context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Step 0: 握手阶段 (5s 超时时间）：包括了读取连接请求、upgrade、auth 整个时间
	step := 0
	timerTask = timer.Add(time.Duration(s.config.Protocol.HandshakeTimeout), func() {
		// 如果 5s 后还没执行完，关闭连接

		// NOTE: 修复 TLS 下的关闭阻塞问题，设置一个 conn deadline
		// 当调用 SetDeadline 时，指定的时间之后，无论连接上是否还有未完成的操作（如数据的读、写或关闭），操作都会超时并返回一个错误。
		// 在某些情况下，特别是在使用 TLS（传输层安全协议）时，关闭连接（即 conn.SendFinishSignal()）可能会出现阻塞。这是因为 TLS 关闭连接时，可能会涉及到额外的握手操作（比如发送 close_notify 消息），这些操作可能会等待对端的响应。
		// 如果对端没有及时响应或连接遇到其他问题，SendFinishSignal() 操作可能会一直阻塞，导致程序无法继续执行。这种情况在网络环境不好的情况下容易发生。
		// 设置一个较短的 Deadline（例如 100 毫秒），确保在一定时间内，连接的关闭操作不会一直阻塞。
		// 100 毫秒是一个比较常见的短时超时时间，足够让正常的关闭操作完成，同时不会让程序因为阻塞时间过长而影响性能或用户体验。
		_ = conn.SetDeadline(time.Now().Add(100 * time.Millisecond))

		_ = conn.Close()
		log.Errorf("key: %s remoteIP: %s step: %d ws handshake timeout", channel.Key, conn.RemoteAddr().String(), step)
	})

	// 获取客户端 IP 地址
	channel.IP, _, _ = net.SplitHostPort(conn.RemoteAddr().String())

	// Step 1: 读取 WebSocket 请求: 从 bufferedReader 中读取到一个完整的 Request (回顾：读取过程中会 fill buffer)
	step = 1
	if wsRequest, err = websocket.ReadRequest(bufferedReader); err != nil || wsRequest.RequestURI != "/sub" {
		// 读取建立 ws 连接请求失败，关闭连接，删除定时器，释放资源
		conn.Close()
		timer.Remove(timerTask)
		readPool.Put(readBuffer)
		if err != io.EOF {
			log.Errorf("http.ReadRequest(bufferedReader) error(%v)", err)
		}
		return
	}

	// 初始化写缓冲区: 延迟到这里才获取，因为才开始要写入
	writeBuffer := writePool.Get()
	channel.Writer.ResetBuffer(conn, writeBuffer.Bytes())

	// Step 2: 请求升级为 WebSocket（返回 HTTP 升级响应）
	step = 2
	if wsConn, err = websocket.Upgrade(conn, bufferedReader, bufferedWriter, wsRequest); err != nil {
		conn.Close()
		timer.Remove(timerTask)
		readPool.Put(readBuffer)
		writePool.Put(writeBuffer)
		if err != io.EOF {
			log.Errorf("websocket.NewServerConn error(%v)", err)
		}
		return
	}

	// 握手步骤 3：认证
	step = 3
	// 从 channel 的 MsgRing 固定 Msg 数组中获取一个可写入的 msg
	if msg, err = channel.MsgRing.GetWriteMsg(); err == nil {
		// WebSocket 认证
		if channel.MemberId, channel.Key, roomID, acceptOps, heartbeatInterval, err = s.authWebsocket(ctx, wsConn, msg, wsRequest.Header.Get("Cookie")); err == nil {
			// 当前用户 channel 监听相关 ops
			channel.Watch(acceptOps...)
			// 返回 channel 对应的 key，哈希选择放入对应的 bucket 中维护改 Channel
			bucket = s.Bucket(channel.Key)
			err = bucket.AddChannel(roomID, channel)
			if conf.Conf.Debug {
				log.Infof("websocket connected key:%s mid:%d proto:%+v", channel.Key, channel.MemberId, msg)
			}
		}
	}

	// 握手步骤 4：处理错误
	step = 4
	if err != nil {
		// 步骤 3 rpc 认证出现错误，关闭连接，删除定时器，释放资源
		wsConn.Close()
		readPool.Put(readBuffer)
		writePool.Put(writeBuffer)
		timer.Remove(timerTask)
		if err != io.EOF && err != websocket.ErrMessageClose {
			log.Errorf("key: %s remoteIP: %s step: %d ws handshake failed error(%v)", channel.Key, conn.RemoteAddr().String(), step, err)
		}
		return
	}

	// 这里已经完成前置握手步骤了（读取连接请求、upgrade、auth），将原有的超时定时任务清除，重置复用为 timerTask 为心跳任务
	timerTask.Key = channel.Key
	// 回调函数依然是关闭连接，只是每次心跳都会重置这个 timer，这样 close 就不会执行了，readPump 中也户已重置这个 timer
	timer.Update(timerTask, heartbeatInterval)

	// 判断是否在白名单中
	isInWhiteList = whitelist.Contains(channel.MemberId)
	if isInWhiteList {
		whitelist.Printf("key: %s[%s] auth\n", channel.Key, roomID)
	}

	// 握手完成，启动写协程处理写入数据
	step = 5
	go s.writePumpWs(wsConn, writePool, writeBuffer, channel)

	// 开始处理协议消息的读取和处理
	serverHeartbeatInterval := s.RandServerHearbeat()
	for {
		// 获取一个用于存放从 conn 读取到的消息的 msg 结构
		if msg, err = channel.MsgRing.GetWriteMsg(); err != nil {
			break
		}

		if isInWhiteList {
			whitelist.Printf("key: %s start read proto\n", channel.Key)
		}

		// 从连接 wsConn 读取数据到 msg 变量中
		if err = msg.ReadWebsocket(wsConn); err != nil {
			break
		}

		if isInWhiteList {
			whitelist.Printf("key: %s read proto:%v\n", channel.Key, msg)
		}

		// 处理心跳消息
		if msg.Op == grpc.OpHeartbeat {
			timer.Update(timerTask, heartbeatInterval)
			msg.Op = grpc.OpHeartbeatReply
			msg.Body = nil

			// 如果超过心跳间隔，发送服务器心跳
			if now := time.Now(); now.Sub(lastHeartbeat) > serverHeartbeatInterval {
				if err1 := s.Heartbeat(ctx, channel.MemberId, channel.Key); err1 == nil {
					lastHeartbeat = now
				}
			}

			if conf.Conf.Debug {
				log.Infof("websocket heartbeat receive key:%s, mid:%d", channel.Key, channel.MemberId)
			}

			step++
		} else {
			// 处理业务消息：这里也会对 msg 进行重置，比如根据 Op 类型，如 OnSub 订阅，则设置 msg.Op = OnSubReply
			// 目前的现状是，每收一条消息，也会给客户端一个回执，可以对消息进行 ack，但目前没有做，只是对 msg.Body 设为 nil
			if err = s.Operate(ctx, msg, channel, bucket); err != nil {
				break
			}
		}

		if isInWhiteList {
			whitelist.Printf("key: %s process proto:%v\n", channel.Key, msg)
		}

		// writePos++
		// 生产者读取到消息后，需要将消息存入 MsgRing，而调用 AdvanceWriteIndex() 则是将写入位置向前移动，准备写入下一个消息
		channel.MsgRing.AdvanceWriteIndex()

		// 发送 ProtoReadReady 信号
		channel.ReadReady()

		if isInWhiteList {
			whitelist.Printf("key: %s msgChan\n", channel.Key)
		}
	}

	// 连接关闭和清理
	if isInWhiteList {
		whitelist.Printf("key: %s server ws error(%v)\n", channel.Key, err)
	}

	// 处理非 EOF 或 WebSocket 关闭的错误
	if err != nil && err != io.EOF && err != websocket.ErrMessageClose && !strings.Contains(err.Error(), "closed") {
		log.Errorf("key: %s server ws failed error(%v)", channel.Key, err)
	}

	// ---------------- 清理资源 开始 -------------------
	// 删除 bucket 中的 channel
	bucket.RemoveChannel(channel)
	// 删除定时器任务
	timer.Remove(timerTask)
	// 关闭 conn 连接（可能多次关闭，WritePump 也会 close）
	wsConn.Close()
	// 往 msg channel 发送 finish 信号，通知 WritePump 关闭
	channel.SendFinishSignal()
	readPool.Put(readBuffer)

	// RPC 发送断开连接信息
	if err = s.Disconnect(ctx, channel.MemberId, channel.Key); err != nil {
		log.Errorf("key: %s operator do disconnect error(%v)", channel.Key, err)
	}
	// ---------------- 清理资源 结束 -------------------

	if isInWhiteList {
		whitelist.Printf("key: %s disconnect error(%v)\n", channel.Key, err)
	}

	if conf.Conf.Debug {
		log.Infof("websocket disconnected key: %s mid:%d", channel.Key, channel.MemberId)
	}
}

func (server *Server) writePumpWs(conn *websocket.Conn, bufferPool *bytes.Pool, buffer *bytes.Buffer, channel *Channel) {
	var (
		err           error
		isFinished    bool                                   // 标记是否完成
		onlineCount   int32                                  // 在线人数
		isWhitelisted = whitelist.Contains(channel.MemberId) // 是否在白名单中
	)

	// 如果开启调试模式，输出调试信息
	if conf.Conf.Debug {
		log.Infof("Channel Key: %s 开始处理 WebSocket 连接", channel.Key)
	}

	// 无限循环读取 channel 中的消息
	for {
		if isWhitelisted {
			whitelist.Printf("Channel Key: %s 等待消息准备好\n", channel.Key)
		}

		// 从 channel 的消息队列中获取消息
		var message = channel.Ready()

		if isWhitelisted {
			whitelist.Printf("Channel Key: %s 消息已准备好\n", channel.Key)
		}

		if conf.Conf.Debug {
			log.Infof("Channel Key:%s 分发消息: %s", channel.Key, message.Body)
		}

		// 根据消息类型处理不同的逻辑
		switch message {
		case grpc.ProtoFinish:
			// 处理结束消息
			if isWhitelisted {
				whitelist.Printf("Channel Key: %s 收到结束消息\n", channel.Key)
			}
			if conf.Conf.Debug {
				log.Infof("Channel Key: %s 唤醒并退出分发协程", channel.Key)
			}
			isFinished = true
			goto cleanup
		case grpc.ProtoReadReady:
			// 从消息环中获取消息
			for {
				// ReadMsg 消息必须 readPump 执行了 writePos++ 才能读，直到 readPos == writePos
				// 正常情况每次收到 ProtoReadReady 信号，因为 writePos++ 了，都会有消息可读
				if message, err = channel.MsgRing.GetReadMsg(); err != nil {
					break
				}

				if isWhitelisted {
					whitelist.Printf("Channel Key: %s 开始向客户端写入消息 %v\n", channel.Key, message)
				}

				// 处理心跳回复消息
				if message.Op == grpc.OpHeartbeatReply {
					if channel.Room != nil {
						onlineCount = channel.Room.OnlineNum() // 获取房间在线人数
					}
					// 写入 WebSocket 心跳消息
					if err = message.WriteWebsocketHeart(conn, onlineCount); err != nil {
						goto cleanup
					}
				} else {
					// 处理其他 MsgRing 中的的消息: 直接写入 conn
					if err = message.WriteWebsocket(conn); err != nil {
						goto cleanup
					}
				}

				if isWhitelisted {
					whitelist.Printf("Channel Key: %s 向客户端写入消息 %v\n", channel.Key, message)
				}

				// 清空消息体，防止内存泄漏
				message.Body = nil
				// 移动读取位置
				channel.MsgRing.AdvanceReadIndex()
			}
		default:
			// 处理服务器发来的普通消息
			if isWhitelisted {
				whitelist.Printf("Channel Key: %s 开始向服务器写入消息 %v\n", channel.Key, message)
			}

			// 向 WebSocket 连接写入数据
			if err = message.WriteWebsocket(conn); err != nil {
				goto cleanup
			}

			if isWhitelisted {
				whitelist.Printf("Channel Key: %s 向服务器写入消息 %v\n", channel.Key, message)
			}

			if conf.Conf.Debug {
				log.Infof("WebSocket 发送消息 Channel Key:%s MemberId:%d Message:%+v", channel.Key, channel.MemberId, message)
			}
		}

		// 消息写入后，进行 flush（刷新）
		if isWhitelisted {
			whitelist.Printf("Channel Key: %s 开始刷新 (flush)\n", channel.Key)
		}

		// 刷新 WebSocket 连接
		if err = conn.Flush(); err != nil {
			break
		}

		if isWhitelisted {
			whitelist.Printf("Channel Key: %s 刷新完成\n", channel.Key)
		}
	}

cleanup:
	// 错误处理，记录错误日志
	if isWhitelisted {
		whitelist.Printf("Channel Key: %s WebSocket 连接出错: %v\n", channel.Key, err)
	}

	if err != nil && err != io.EOF && err != websocket.ErrMessageClose {
		log.Errorf("Channel Key: %s WebSocket 连接出错: %v", channel.Key, err)
	}

	// 关闭 WebSocket 连接，WritePump 这里关闭了
	// ReadPump 的 for 循环在 ReadWebsocket 的时候就会报错并退出循环，执行清理并发送 ProtoFinish
	conn.Close()

	// 将 write buffer 放回 bufferPool
	bufferPool.Put(buffer)

	// 等待收到 ReadPump 发来的 ProtoFinish 信号
	for !isFinished {
		isFinished = (channel.Ready() == grpc.ProtoFinish)
	}

	// 输出调试信息，协程退出
	if conf.Conf.Debug {
		log.Infof("Channel Key: %s WebSocket 处理协程退出", channel.Key)
	}
}

// auth for goim handshake with client, use rsa & aes.
func (s *Server) authWebsocket(ctx context.Context, conn *websocket.Conn, msg *grpc.ProtoMsg, cookie string) (memberID int64, key, roomID string, acceptOps []int32, heartbeatInterval time.Duration, err error) {
	for {
		// 读取连接
		if err = msg.ReadWebsocket(conn); err != nil {
			return
		}

		// 读到的 msg 是一个 OpAuth 的消息
		if msg.Op == grpc.OpAuth {
			break
		} else {
			log.Errorf("conn request operation(%d) not auth", msg.Op)
		}
	}

	// 根据 cookie 调 rpc 获取用户信息
	if memberID, key, roomID, acceptOps, heartbeatInterval, err = s.Connect(ctx, msg, cookie); err != nil {
		return
	}

	// 消息的操作类型：auth 响应
	msg.Op = grpc.OpAuthReply
	// 无 Body
	msg.Body = nil

	// 将 msg 写入到 bufferedWriter 的 buffer 中
	if err = msg.WriteWebsocket(conn); err != nil {
		return
	}

	// 最终 flush 刷到 conn
	err = conn.Flush()
	return
}
