package comet

import (
	"github.com/Terry-Mao/goim/api/comet/grpc"
	"github.com/Terry-Mao/goim/internal/comet/conf"
	"github.com/Terry-Mao/goim/internal/comet/errors"
	log "github.com/golang/glog"
)

// MsgRingBuffer 环形缓冲区，用于存储协议消息
type MsgRingBuffer struct {
	// 读指针，读指针在前
	readPos uint64
	// 写指针，写指针在后，其实这里并不是真的「写」，而是按顺序取一个用于存放数据的 msg 结构
	writePos uint64
	// 缓冲区大小，必须是 2 的幂
	bufferSize uint64
	// 掩码，用于快速取模 (bufferSize - 1)
	indexMask uint64
	// 存储协议消息的数组
	messages []grpc.ProtoMsg
}

// NewMsgRing 创建一个新的环形缓冲区
func NewMsgRing(num int) *MsgRingBuffer {
	r := new(MsgRingBuffer)
	r.init(uint64(num))
	return r
}

// Init 初始化环形缓冲区
func (r *MsgRingBuffer) Init(bufferSize int) {
	r.init(uint64(bufferSize))
}

// init 初始化环形缓冲区，bufferSize 必须是 2 的幂
func (r *MsgRingBuffer) init(bufferSize uint64) {
	// 如果 bufferSize 不是 2 的幂，则调整为最近的 2 的幂
	if bufferSize&(bufferSize-1) != 0 {
		for bufferSize&(bufferSize-1) != 0 {
			bufferSize &= (bufferSize - 1)
		}
		bufferSize = bufferSize << 1
	}
	// 分配缓冲区空间
	r.messages = make([]grpc.ProtoMsg, bufferSize)
	r.bufferSize = bufferSize
	r.indexMask = r.bufferSize - 1
}

// GetReadMsg 从环形缓冲区获取一个协议消息
func (r *MsgRingBuffer) GetReadMsg() (msg *grpc.ProtoMsg, err error) {
	// 如果读指针追上写指针，表示缓冲区为空
	if r.readPos == r.writePos {
		return nil, errors.ErrRingEmpty
	}
	// 返回当前读指针 readPos 指向的协议消息
	msg = &r.messages[r.readPos&r.indexMask]
	return
}

// AdvanceReadIndex 前进读指针
func (r *MsgRingBuffer) AdvanceReadIndex() {
	r.readPos++
	if conf.Conf.Debug {
		log.Infof("ring readPos: %d, idx: %d", r.readPos, r.readPos&r.indexMask)
	}
}

// GetWriteMsg 获取一个可写入的协议消息槽位
func (r *MsgRingBuffer) GetWriteMsg() (msg *grpc.ProtoMsg, err error) {
	// 如果写指针和读指针的差值达到缓冲区大小，表示缓冲区已满
	if r.writePos-r.readPos >= r.bufferSize {
		return nil, errors.ErrRingFull
	}
	// 返回当前写指针指向的协议消息槽位
	msg = &r.messages[r.writePos&r.indexMask]
	return
}

// AdvanceWriteIndex 前进写指针
func (r *MsgRingBuffer) AdvanceWriteIndex() {
	r.writePos++
	if conf.Conf.Debug {
		log.Infof("ring writePos: %d, idx: %d", r.writePos, r.writePos&r.indexMask)
	}
}

// ResetBuffer 重置环形缓冲区
func (r *MsgRingBuffer) ResetBuffer() {
	r.readPos = 0
	r.writePos = 0
	// 防止编译器优化掉 pad 字段
	// r.pad = [40]byte{}
}
