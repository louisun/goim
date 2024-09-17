package comet

import (
	"github.com/Terry-Mao/goim/internal/comet/conf"
	"github.com/Terry-Mao/goim/pkg/bytes"
	"github.com/Terry-Mao/goim/pkg/time"
)

// RoundOptions round options.
type RoundOptions struct {
	Timer        int
	TimerSize    int
	Reader       int
	ReadBuf      int
	ReadBufSize  int
	Writer       int
	WriteBuf     int
	WriteBufSize int
}

// Round userd for connection round-robin get a reader/writer/timer for split big lock.
type Round struct {
	// 默认参数：每个 Pool 1024 个 Buffer, 每个 Buffer 底层 8KB 字节，总共 8MB
	// 32 个 Pool，总共 256MB
	readers []bytes.Pool
	// 默认参数：每个 Pool 1024 个 Buffer, 每个 Buffer 底层 8KB 字节，总共 8MB
	// 32 个 Pool，总共 256MB
	writers []bytes.Pool
	// 默认参数：32 个 Timer (自定义结构），每个 Timer 结构有 2048 个 timerTask
	timers  []time.Timer
	options RoundOptions
}

// NewRound new a round struct.
func NewRound(c *conf.Config) (r *Round) {
	var i int
	r = &Round{
		options: RoundOptions{
			Reader:       c.TCP.Reader,         // 32 []Pool
			ReadBuf:      c.TCP.ReadBuf,        // 1K Buffer
			ReadBufSize:  c.TCP.ReadBufSize,    // 8K ss]byte
			Writer:       c.TCP.Writer,         // 32 []Pool
			WriteBuf:     c.TCP.WriteBuf,       // 1K Buffer
			WriteBufSize: c.TCP.WriteBufSize,   // 8K []byte
			Timer:        c.Protocol.Timer,     // 32 []Timer
			TimerSize:    c.Protocol.TimerSize, // 2048 (task size)
		}}
	// reader
	r.readers = make([]bytes.Pool, r.options.Reader)
	for i = 0; i < r.options.Reader; i++ {
		r.readers[i].Init(r.options.ReadBuf, r.options.ReadBufSize)
	}
	// writer
	r.writers = make([]bytes.Pool, r.options.Writer)
	for i = 0; i < r.options.Writer; i++ {
		r.writers[i].Init(r.options.WriteBuf, r.options.WriteBufSize)
	}
	// timer
	r.timers = make([]time.Timer, r.options.Timer)
	for i = 0; i < r.options.Timer; i++ {
		r.timers[i].Init(r.options.TimerSize)
	}
	return
}

// Timer get a timer.
func (r *Round) Timer(rn int) *time.Timer {
	return &(r.timers[rn%r.options.Timer])
}

// Reader get a reader memory buffer.
func (r *Round) Reader(rn int) *bytes.Pool {
	return &(r.readers[rn%r.options.Reader])
}

// Writer get a writer memory buffer pool.
func (r *Round) Writer(rn int) *bytes.Pool {
	return &(r.writers[rn%r.options.Writer])
}
