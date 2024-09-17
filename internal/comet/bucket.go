package comet

import (
	"sync"
	"sync/atomic"

	"github.com/Terry-Mao/goim/api/comet/grpc"
	"github.com/Terry-Mao/goim/internal/comet/conf"
)

// Bucket 是一个管理 Channel 和 Room 的容器
type Bucket struct {
	// Bucket 配置
	config *conf.Bucket

	// Channel Map，key 为连接的 Key，值为对应的 Channel，管理当前 bucket 下所有的 Channel
	mu         sync.RWMutex
	channelMap map[string]*Channel

	// 管理当前 bucket 下所有的 Room
	// Room Map，key 为 RoomID，值为对应的 Room
	// Room 维护了房间内的 Channel -> Channel -> ... 链表，以及人数等统计信息
	roomMap map[string]*Room
	// 房间消息请求队列，默认 32 个协程，每个 channel 1024 个大小
	routines []chan *grpc.BroadcastRoomReq
	// 推送房间消息的协程数量
	routineCount uint64

	// 每个 IP 地址的活跃连接计数，防止单个 IP 建立过多连接
	ipConnectionCount map[string]int32
}

// NewBucket 新建一个 Bucket 实例，初始化相关数据结构
func NewBucket(config *conf.Bucket) *Bucket {
	b := &Bucket{
		config: config,
		// 默认 1024 个 Channel map 大小
		channelMap:        make(map[string]*Channel, config.Channel),
		ipConnectionCount: make(map[string]int32),
		// 默认 1024 个大小
		roomMap: make(map[string]*Room, config.Room),
		// 默认 32 个协程
		routines: make([]chan *grpc.BroadcastRoomReq, config.RoutineAmount),
	}

	// 初始化房间消息处理的协程
	for i := uint64(0); i < config.RoutineAmount; i++ {
		// 每个 channel 1024 容量
		roomChan := make(chan *grpc.BroadcastRoomReq, config.RoutineSize)
		b.routines[i] = roomChan
		// 32 个协程
		go b.roomChanProcessor(roomChan)
	}

	return b
}

// ChannelCount 返回当前 Bucket 中的 Channel 数量
func (b *Bucket) ChannelCount() int {
	return len(b.channelMap)
}

// RoomCount 返回当前 Bucket 中的 Room 数量
func (b *Bucket) RoomCount() int {
	return len(b.roomMap)
}

// RoomsCount 返回所有在线人数大于 0 的房间及其在线人数
func (b *Bucket) RoomsCount() map[string]int32 {
	result := make(map[string]int32)
	b.mu.RLock()
	for roomID, room := range b.roomMap {
		if room.Online > 0 {
			result[roomID] = room.Online
		}
	}
	b.mu.RUnlock()
	return result
}

// ChangeRoom 切换 Channel 所在的 Room
func (b *Bucket) ChangeRoom(newRoomID string, channel *Channel) error {
	oldRoom := channel.Room

	// 如果新房间 ID 为空，表示移除房间
	if newRoomID == "" {
		if oldRoom != nil && oldRoom.RemoveChannel(channel) {
			b.deleteRoom(oldRoom) // 删除旧房间
		}
		channel.Room = nil
		return nil
	}

	// 获取或创建新房间
	b.mu.Lock()
	newRoom, exists := b.roomMap[newRoomID]
	if !exists {
		newRoom = NewRoom(newRoomID)
		b.roomMap[newRoomID] = newRoom
	}
	b.mu.Unlock()

	// 将 Channel 加入新房间
	if err := newRoom.AddChannel(channel); err != nil {
		return err
	}
	channel.Room = newRoom

	// 从旧房间移除
	if oldRoom != nil && oldRoom.RemoveChannel(channel) {
		b.deleteRoom(oldRoom)
	}

	return nil
}

// AddChannel 将一个 Channel 加入 Bucket 中，并根据 RoomID 加入对应的 Room
func (b *Bucket) AddChannel(roomID string, channel *Channel) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 如果已有相同 Key 的旧 Channel，关闭它
	if oldChannel := b.channelMap[channel.Key]; oldChannel != nil {
		oldChannel.SendFinishSignal()
	}
	b.channelMap[channel.Key] = channel

	// 加入房间
	var room *Room
	var exists bool
	if roomID != "" {
		if room, exists = b.roomMap[roomID]; !exists {
			room = NewRoom(roomID)
			b.roomMap[roomID] = room
		}
		channel.Room = room
	}

	// 更新 IP 连接计数
	b.ipConnectionCount[channel.IP]++

	// 将 Channel 加入 Room
	if room != nil {
		return room.AddChannel(channel)
	}

	return nil
}

// RemoveChannel 删除指定的 Channel，并从 Room 中移除
func (b *Bucket) RemoveChannel(channel *Channel) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 从 Channel Map 中移除
	if existingChannel, ok := b.channelMap[channel.Key]; ok {
		room := existingChannel.Room
		if existingChannel == channel {
			delete(b.channelMap, channel.Key)
		}

		// 更新 IP 连接计数
		if b.ipConnectionCount[channel.IP] > 1 {
			b.ipConnectionCount[channel.IP]--
		} else {
			delete(b.ipConnectionCount, channel.IP)
		}

		// 从 Room 中移除 Channel，如果 Room 为空则删除 Room
		if room != nil && room.RemoveChannel(existingChannel) {
			b.deleteRoom(room)
		}
	}
}

// Channel 根据 Key 获取 Channel
func (b *Bucket) Channel(key string) *Channel {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.channelMap[key]
}

// Broadcast 向 Bucket 中的所有 Channel 广播消息
func (b *Bucket) Broadcast(message *grpc.ProtoMsg, operation int32) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, channel := range b.channelMap {
		if channel.NeedPush(operation) {
			_ = channel.Push(message) // 忽略错误处理
		}
	}
}

// Room 根据 RoomID 获取 Room
func (b *Bucket) Room(roomID string) *Room {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.roomMap[roomID]
}

// deleteRoom 删除指定的 Room
func (b *Bucket) deleteRoom(room *Room) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.roomMap, room.ID)
	room.Close()
}

// BroadcastRoom 向指定 RoomID 的 Room 广播消息
func (b *Bucket) BroadcastRoom(request *grpc.BroadcastRoomReq) {
	routineID := atomic.AddUint64(&b.routineCount, 1) % b.config.RoutineAmount
	b.routines[routineID] <- request
}

// Rooms 获取所有在线人数大于 0 的房间 ID
func (b *Bucket) Rooms() map[string]struct{} {
	result := make(map[string]struct{})
	b.mu.RLock()
	for roomID, room := range b.roomMap {
		if room.Online > 0 {
			result[roomID] = struct{}{}
		}
	}
	b.mu.RUnlock()
	return result
}

// IPCount 获取所有连接 IP 的计数
func (b *Bucket) IPCount() map[string]struct{} {
	result := make(map[string]struct{})
	b.mu.RLock()
	for ip := range b.ipConnectionCount {
		result[ip] = struct{}{}
	}
	b.mu.RUnlock()
	return result
}

// UpRoomsCount 更新每个房间的在线人数
func (b *Bucket) UpRoomsCount(roomCountMap map[string]int32) {
	b.mu.RLock()
	for roomID, room := range b.roomMap {
		if count, ok := roomCountMap[roomID]; ok {
			room.AllOnline = count
		}
	}
	b.mu.RUnlock()
}

// roomChanProcessor 处理房间消息的协程
func (b *Bucket) roomChanProcessor(roomChan chan *grpc.BroadcastRoomReq) {
	for request := range roomChan {
		// 房间存在，就推送房间消息
		if room := b.Room(request.RoomID); room != nil {
			// 遍历 room 中 Channel 链表，每个 Channel 都推送消息到 msgChan 中
			room.PushMsg(request.Proto)
		}
	}
}
