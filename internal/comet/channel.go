package comet

import (
	"sync"

	"github.com/Terry-Mao/goim/api/comet/grpc"
	"github.com/Terry-Mao/goim/pkg/bufio"
)

// Channel 用于消息推送，消息通过写入协程发送。
type Channel struct {
	// 每个 channel 属于一个 room
	Room *Room

	// 每个 channel 拥有一个环形缓冲区，用于存储客户端协议数据
	MsgRing MsgRingBuffer

	// 带缓冲的信号通道，Ready() 从这里读取要处理的消息
	msgChan chan *grpc.ProtoMsg

	// 绑定的写入和读取缓冲区，与连接 (conn) 相关联
	Writer bufio.BufferedWriter
	Reader bufio.BufferedReader

	// 链表，用于管理多个 channel
	Next *Channel
	Prev *Channel

	// 用户的唯一标识
	MemberId int64
	// 用户的唯一 key
	Key string
	// 用户的 IP 地址
	IP string

	// 存储该 channel 监听的操作类型
	watchOps map[int32]struct{}
	// 读写锁，保护 watchOps
	mutex sync.RWMutex
}

// NewChannel 创建一个新的 channel。
func NewChannel(msgRingBufferSize, msgChannelSize int) *Channel {
	c := new(Channel)
	// 初始化客户端协议缓冲区
	c.MsgRing.Init(msgRingBufferSize)
	// 初始化带缓冲的信号通道
	c.msgChan = make(chan *grpc.ProtoMsg, msgChannelSize)
	// 初始化操作类型的监听表
	c.watchOps = make(map[int32]struct{})
	return c
}

// Watch 监听一组操作类型。
func (c *Channel) Watch(ops ...int32) {
	c.mutex.Lock()
	for _, op := range ops {
		// 将指定操作类型加入监听表
		c.watchOps[op] = struct{}{}
	}
	c.mutex.Unlock()
}

// UnWatch 取消监听一组操作类型。
func (c *Channel) UnWatch(ops ...int32) {
	c.mutex.Lock()
	for _, op := range ops {
		// 从监听表中删除指定操作类型
		delete(c.watchOps, op)
	}
	c.mutex.Unlock()
}

// NeedPush 判断指定操作类型是否被监听。
func (c *Channel) NeedPush(op int32) bool {
	c.mutex.RLock()
	// 如果操作类型存在于监听表中，返回 true
	_, ok := c.watchOps[op]
	c.mutex.RUnlock()
	return ok
}

// Push 服务端推送消息到 channel。
func (c *Channel) Push(p *grpc.ProtoMsg) (err error) {
	select {
	case c.msgChan <- p: // 尝试将消息推送到信号通道
	default: // 如果通道已满，消息将被丢弃
	}
	return
}

// Ready 从信号通道中读取并返回下一个准备好的消息。
func (c *Channel) Ready() *grpc.ProtoMsg {
	return <-c.msgChan
}

// ReadReady 向 channel 发送信号，表示有消息准备就绪。
func (c *Channel) ReadReady() {
	c.msgChan <- grpc.ProtoReadReady
}

// SendFinishSignal 关闭 channel，发送关闭协议。
func (c *Channel) SendFinishSignal() {
	c.msgChan <- grpc.ProtoFinish
}
