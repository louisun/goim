package bytes

import (
	"sync"
)

// Buffer buffer.
type Buffer struct {
	buf  []byte
	next *Buffer // next free buffer
}

// Bytes bytes.
func (b *Buffer) Bytes() []byte {
	return b.buf
}

// Pool is a buffer pool.
type Pool struct {
	lock           sync.Mutex
	free           *Buffer // 指向当前空闲的缓冲区，构成了一个链表的头节点
	maxAllByte     int     // 表示池中所有缓冲区的总字节数，maxAllByte = bufferNum * eachBufferSize
	bufferNum      int     // 表示池中的缓冲区数量
	eachBufferSize int     // 每一个缓冲区的大小
}

// NewPool new a memory buffer pool struct.
func NewPool(num, size int) (p *Pool) {
	p = new(Pool)
	p.init(num, size)
	return
}

// Init init the memory buffer.
func (p *Pool) Init(num, size int) {
	p.init(num, size)
}

// init init the memory buffer.
func (p *Pool) init(bufferNum, eachBufferSize int) {
	p.bufferNum = bufferNum
	p.eachBufferSize = eachBufferSize
	p.maxAllByte = bufferNum * eachBufferSize
	p.grow()
}

// grow grow the memory buffer eachBufferSize, and update free pointer.
// grow 扩展内存缓冲池容量，并更新空闲链表的指针
func (p *Pool) grow() {
	var (
		index         int      // 当前缓冲区的索引
		currentBuffer *Buffer  // 当前操作的缓冲区
		buffers       []Buffer // 新创建的缓冲区数组
		totalMemory   []byte   // 分配的总内存块，用于分配给每个缓冲区
	)

	// 一次性分配 num * size 的内存空间，用于创建多个缓冲区
	totalMemory = make([]byte, p.maxAllByte)

	// 创建 num 个 Buffer 实例
	buffers = make([]Buffer, p.bufferNum)

	// 将空闲链表的起始位置指向第一个缓冲区
	p.free = &buffers[0]
	currentBuffer = p.free

	// 将分配的总内存按照每个缓冲区的大小分块，并连接成链表
	for index = 1; index < p.bufferNum; index++ {
		// 将总内存的每一段分配给对应的 Buffer（切片操作）
		currentBuffer.buf = totalMemory[(index-1)*p.eachBufferSize : index*p.eachBufferSize]

		// 将当前缓冲区的 next 指向下一个缓冲区
		currentBuffer.next = &buffers[index]

		// 移动到下一个缓冲区
		currentBuffer = currentBuffer.next
	}

	// 为最后一个缓冲区分配内存
	currentBuffer.buf = totalMemory[(index-1)*p.eachBufferSize : index*p.eachBufferSize]

	// 最后一个缓冲区的 next 设置为 nil，表示链表结束
	currentBuffer.next = nil
}

// Get get a free memory buffer.
func (p *Pool) Get() (b *Buffer) {
	p.lock.Lock()
	if b = p.free; b == nil {
		p.grow()
		b = p.free
	}
	p.free = b.next
	p.lock.Unlock()
	return
}

// Put put back a memory buffer to free.
func (p *Pool) Put(b *Buffer) {
	p.lock.Lock()
	b.next = p.free
	p.free = b
	p.lock.Unlock()
}
