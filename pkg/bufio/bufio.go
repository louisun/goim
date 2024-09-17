// bufio 包实现了带缓冲的 I/O 操作。它包装了 io.BufferedReader 或 io.BufferedWriter 对象，创建了一个带缓冲的对象
// 在执行 I/O 操作时提升了效率，尤其在进行文本处理时。
package bufio

import (
	"bytes"
	"errors"
	"io"
)

const (
	// 默认缓冲区大小
	defaultBufferSize = 4096
)

var (
	// ErrInvalidUnreadByte invalid use of UnreadByete
	ErrInvalidUnreadByte = errors.New("bufio: invalid use of UnreadByte")
	// ErrInvalidUnreadRune invalid use of UnreadRune
	ErrInvalidUnreadRune = errors.New("bufio: invalid use of UnreadRune")
	// ErrBufferFull buffer full
	ErrBufferFull = errors.New("bufio: buffer full")
	// ErrNegativeCount negative count
	ErrNegativeCount = errors.New("bufio: negative count")
)

// 带缓冲输入处理

// BufferedReader 实现了对 io.BufferedReader 的缓冲读取
type BufferedReader struct {
	buffer            []byte    // 缓冲区
	underlyingReader  io.Reader // 底层的 io.BufferedReader
	readPos, writePos int       // 缓冲区的读取和写入位置
	readError         error     // 记录读取时的错误
}

// 最小缓冲区大小
const minBufferSize = 16

// 最大连续空读取次数
const maxConsecutiveEmptyReads = 100

// NewBufferedReaderSize 返回一个带指定缓冲区大小的 BufferedReader。
// 如果传入的 io.BufferedReader 已经是一个足够大的 BufferedReader，直接返回它。
func NewBufferedReaderSize(rd io.Reader, size int) *BufferedReader {
	// 检查是否已经是 BufferedReader
	existingReader, ok := rd.(*BufferedReader)
	if ok && len(existingReader.buffer) >= size {
		return existingReader
	}
	if size < minBufferSize {
		size = minBufferSize
	}
	r := new(BufferedReader)
	r.resetBuffer(make([]byte, size), rd)
	return r
}

// NewBufferedReader 返回一个默认缓冲区大小的 BufferedReader
func NewBufferedReader(rd io.Reader) *BufferedReader {
	return NewBufferedReaderSize(rd, defaultBufferSize)
}

// Reset 重置缓冲区并将底层 BufferedReader 切换为新的 BufferedReader
func (r *BufferedReader) Reset(newReader io.Reader) {
	r.resetBuffer(r.buffer, newReader)
}

// ResetBuffer 重置缓冲区为指定的缓冲区，并将底层 BufferedReader 切换为新的 BufferedReader
func (r *BufferedReader) ResetBuffer(newReader io.Reader, newBuffer []byte) {
	r.resetBuffer(newBuffer, newReader)
}

// resetBuffer 是一个私有函数，用于重置缓冲区和 BufferedReader
func (r *BufferedReader) resetBuffer(buf []byte, rd io.Reader) {
	*r = BufferedReader{
		buffer:           buf,
		underlyingReader: rd,
	}
}

var errNegativeRead = errors.New("bufio: 底层 BufferedReader 返回了负数的读取字节数")

// fill 方法的作用是将新数据从底层的 io.Reader 读取到缓冲区 buffer 中。
// 如果缓冲区中已经有部分数据（即 readPos 可能不为 0），它会将未读取的数据前移，
// 以便腾出更多的空间来容纳新的数据。同时，它会尝试多次从底层读取，直到成功读取数据或遇到错误。
func (r *BufferedReader) fill() {
	// 数据前移（readPos = 0, writePos = ?）
	if r.readPos > 0 {
		copy(r.buffer, r.buffer[r.readPos:r.writePos])
		r.writePos -= r.readPos
		r.readPos = 0
	}

	// 假设一开始 readPos == 0，writePos = len(r.buffer) 就存在这种情况
	// 表示缓冲区已经满了，不应该去从 reader 读取
	// 正确的使用方式是先消费才fill，出现这种情况说明使用方式不对，应该 panic
	if r.writePos >= len(r.buffer) {
		panic("bufio: tried to fill full buffer")
	}

	// 默认 100 次读取
	for i := maxConsecutiveEmptyReads; i > 0; i-- {
		// 将数据从 reader 读取到 writePos 开始的位置
		n, err := r.underlyingReader.Read(r.buffer[r.writePos:])

		// n 表示读取的字节数
		if n < 0 {
			panic(errNegativeRead)
		}

		// writePos 后移
		r.writePos += n

		// 记录 GetReadMsg error
		if err != nil {
			r.readError = err
			return
		}

		// 只要读取数据成功（不论多少字节）都结束
		if n > 0 {
			return
		}
	}

	// 100 次读取都没读到数据，返回“无进展”错误
	r.readError = io.ErrNoProgress
}

// readErr 返回读取时的错误并清空错误状态
func (r *BufferedReader) readErr() error {
	err := r.readError
	r.readError = nil
	return err
}

// Peek 返回接下来的 n 个字节，而不移动读取位置。
// 如果返回的字节数少于 n，则返回错误。
func (r *BufferedReader) Peek(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrNegativeCount
	}

	// 无法读取超过 buffer 长度的数据
	if n > len(r.buffer) {
		return nil, ErrBufferFull
	}

	// for 循环：缓冲区未满 n（读写指针只差）fill 从 Reader 读取填充数据，直到缓冲区至少满足 n 个字节，或遇到 readError
	for r.writePos-r.readPos < n && r.readError == nil {
		r.fill()
	}

	// fill 之后，如果缓冲区 buffer 还是中没有足够的数据 (writePos - readPos 满足 n 个字节），记录错误，只返回 avail 的字节数
	var err error
	if avail := r.writePos - r.readPos; avail < n {
		n = avail
		err = r.readErr()
		if err == nil {
			err = ErrBufferFull
		}
	}

	// 返回对应位置 []byte 和 err
	return r.buffer[r.readPos : r.readPos+n], err
}

// Pop 返回接下来的 n 个字节，并移动读取位置。
// 如果返回的字节数少于 n，则返回错误。
func (r *BufferedReader) Pop(n int) ([]byte, error) {
	data, err := r.Peek(n)
	if err == nil {
		r.readPos += n
		return data, err
	}
	return nil, err
}

// Discard 跳过接下来的 n 个字节，返回跳过的字节数和可能的错误。
func (r *BufferedReader) Discard(n int) (discarded int, err error) {
	if n < 0 {
		return 0, ErrNegativeCount
	}
	if n == 0 {
		return
	}
	remain := n
	for {
		skip := r.Buffered()
		if skip == 0 {
			r.fill()
			skip = r.Buffered()
		}
		if skip > remain {
			skip = remain
		}
		r.readPos += skip
		remain -= skip
		if remain == 0 {
			return n, nil
		}
		if r.readError != nil {
			return n - remain, r.readErr()
		}
	}
}

// Read 从缓冲区中读取数据到 destBytes 中(最多 len(destBytes) 的字节数)，返回读取的字节数
func (r *BufferedReader) Read(destBytes []byte) (n int, err error) {
	// 最多读取的字节数 n 为传入的切片长度
	n = len(destBytes)
	if n == 0 {
		return 0, r.readErr()
	}

	// 缓冲区为空
	if r.readPos == r.writePos {
		if r.readError != nil {
			return 0, r.readErr()
		}

		// 如果需要读取的字节数 n 甚至大于 buffer 长度，就没必要 fill 了，直接从底层 Reader GetReadMsg 可以避免拷贝
		if n >= len(r.buffer) {
			n, r.readError = r.underlyingReader.Read(destBytes)
			if n < 0 {
				panic(errNegativeRead)
			}

			return n, r.readErr()
		}

		// n 还是小于 buffer 长度的，则走 fill() 缓冲区为空填充的逻辑
		r.fill()

		// fill 完了还是为空直接返回
		if r.readPos == r.writePos {
			return 0, r.readErr()
		}
	}

	// 直接 copy buffer 读写指针之间的数据到目标 destBytes 切片中
	n = copy(destBytes, r.buffer[r.readPos:r.writePos])
	// 移动 readPos 指针
	r.readPos += n
	return n, nil
}

// ReadByte 从缓冲区中读取并返回一个字节
func (r *BufferedReader) ReadByte() (byte, error) {
	for r.readPos == r.writePos {
		if r.readError != nil {
			return 0, r.readErr()
		}
		r.fill() // 缓冲区为空，填充
	}
	c := r.buffer[r.readPos]
	r.readPos++
	return c, nil
}

// ReadDelim 读取直到遇到指定的分隔符 delim，返回一个指向缓冲区的切片
func (r *BufferedReader) ReadDelim(delim byte) (line []byte, err error) {
	for {
		// 在缓冲区中查找分隔符 delim
		if i := bytes.IndexByte(r.buffer[r.readPos:r.writePos], delim); i >= 0 {
			line = r.buffer[r.readPos : r.readPos+i+1]
			r.readPos += i + 1
			break
		}

		// 缓冲区中没有 delim，之前 fill 存在错误，返回数据和 err
		if r.readError != nil {
			line = r.buffer[r.readPos:r.writePos]
			r.readPos = r.writePos
			err = r.readErr()
			break
		}

		// 缓冲区中没有 delim，且缓冲区已满，返回数据和 err
		// 其实这里，业务数据中需要保证这个分隔符出现的频率，一定得在 len(r.buffer) 之内，不然不适合用此方法读取分隔符
		if r.Buffered() >= len(r.buffer) {
			r.readPos = r.writePos
			line = r.buffer
			err = ErrBufferFull
			break
		}

		// 缓冲区未满，继续 fill 填充，并进入下一个循环
		r.fill()
	}

	return
}

// ReadLine 读取一行，并返回它的字节切片，不包含换行符 (CRLF)。
// 返回值：
//   - line: 读取到的行内容。
//   - more: 如果缓冲区已满，但尚未找到完整的行（即未找到换行符），此值为 true。
//   - err: 发生错误时返回相应的错误；如果读取成功，则为 nil。
func (r *BufferedReader) ReadLine() (line []byte, more bool, err error) {
	// 使用 ReadDelim 函数读取直到遇到换行符 '\n' 为止。
	line, err = r.ReadDelim('\n')

	// 如果 ReadDelim 遇到缓冲区满，且没有读到换行符 '\n'，则直接返回已读取的数据。
	// 这种情况通常意味着缓冲区中的内容超过了缓冲区大小而没有遇到换行符，导致未能读取完整的一行。
	if errors.Is(err, ErrBufferFull) {
		// 特殊情况处理：如果最后一个字符是 '\r'，需要考虑到后面可能会出现 '\r\n' 组合。
		// 因为在网络协议或某些数据格式中，换行符经常表示为 '\r\n' (CRLF)。
		if len(line) > 0 && line[len(line)-1] == '\r' {
			// 为什么要执行 r.readPos--？
			// 如果缓冲区满了，且最后一个读取的字符是 '\r'，有可能后面会有 '\n'，但由于缓冲区已满，我们没有读取到它。
			// 因此通过将 readPos 回退 1 个位置，以便下一次读取时可以重新处理这个 '\r' 和可能后续的 '\n'。
			// 这避免了丢失 '\n'，保证数据完整性。

			if r.readPos == 0 {
				// 理论上不应该出现这种情况：即尝试回退到缓冲区的起始位置
				// 因为缓冲区已满，且 readPos 不应为 0。
				panic("bufio: tried to rewind past start of buffer")
			}

			// 回退 readPos，使下次读取时可以重新处理 '\r'
			r.readPos--
			// 同时将 '\r' 从当前行中移除，因为它可能是 CRLF 的组成部分。
			line = line[:len(line)-1]
		}

		// 返回已读取的部分行，同时设置 more 为 true，表示该行未读取完整。
		return line, true, nil
	}

	// 如果未读取到任何数据
	if len(line) == 0 {
		// 如果同时有错误，则返回错误和 nil 数据
		if err != nil {
			line = nil
		}
		return
	}

	// 成功读取数据，将 err 置为 nil，表示没有错误
	err = nil

	// 如果最后一个字符是 '\n'，需要移除它（以及可能存在的 '\r'）。
	if line[len(line)-1] == '\n' {
		drop := 1 // 默认移除 '\n'
		// 如果倒数第二个字符是 '\r'，则移除 '\r\n'
		if len(line) > 1 && line[len(line)-2] == '\r' {
			drop = 2
		}
		// 截取 line，去掉结尾的 '\r\n' 或 '\n'
		line = line[:len(line)-drop]
	}

	// 返回读取到的一行
	return
}

// Buffered 返回缓冲区中可以读取的字节数
func (r *BufferedReader) Buffered() int { return r.writePos - r.readPos }

// BufferedWriter 实现了对 io.BufferedWriter 的缓冲写入
type BufferedWriter struct {
	writeError       error
	buffer           []byte
	writePos         int
	underlyingWriter io.Writer
}

// NewBufferedWriterSize 返回一个带指定缓冲区大小的 BufferedWriter。
// 如果传入的 io.BufferedWriter 已经是一个足够大的 BufferedWriter，直接返回它。
func NewBufferedWriterSize(w io.Writer, size int) *BufferedWriter {
	// 检查是否已经是 BufferedWriter
	existingWriter, ok := w.(*BufferedWriter)
	if ok && len(existingWriter.buffer) >= size {
		return existingWriter
	}
	if size <= 0 {
		size = defaultBufferSize
	}
	return &BufferedWriter{
		buffer:           make([]byte, size),
		underlyingWriter: w,
	}
}

// NewBufferedWriter 返回一个默认缓冲区大小的 BufferedWriter
func NewBufferedWriter(w io.Writer) *BufferedWriter {
	return NewBufferedWriterSize(w, defaultBufferSize)
}

// Reset 重置缓冲区并将底层 BufferedWriter 切换为新的 BufferedWriter
func (w *BufferedWriter) Reset(newWriter io.Writer) {
	w.writeError = nil
	w.writePos = 0
	w.underlyingWriter = newWriter
}

// ResetBuffer 重置缓冲区为指定的缓冲区，并将底层 BufferedWriter 切换为新的 BufferedWriter
func (w *BufferedWriter) ResetBuffer(newWriter io.Writer, newBuffer []byte) {
	w.buffer = newBuffer
	w.writeError = nil
	w.writePos = 0
	w.underlyingWriter = newWriter
}

// Flush 将缓冲区中的数据写入底层的 io.BufferedWriter
func (w *BufferedWriter) Flush() error {
	return w.flush()
}

// flush 是一个私有函数，用于将缓冲区中的数据写入底层 BufferedWriter
func (w *BufferedWriter) flush() error {
	// 如果之前的写入操作已经返回错误，直接返回该错误
	if w.writeError != nil {
		return w.writeError
	}
	// 如果缓冲区中没有数据要写（写入位置为0），则不需要任何操作，直接返回
	if w.writePos == 0 {
		return nil
	}
	// 将缓冲区中的数据写入底层的实际writer
	n, err := w.underlyingWriter.Write(w.buffer[0:w.writePos])
	// 如果写入的字节数少于预期且没有返回错误，标记为短写错误
	if n < w.writePos && err == nil {
		err = io.ErrShortWrite
	}
	// 如果写入过程中发生了错误
	if err != nil {
		// 如果部分数据已经成功写入，剩余的数据需要保存到缓冲区的前面部分
		if n > 0 && n < w.writePos {
			// 将未写入的数据移动到缓冲区的前面
			copy(w.buffer[0:w.writePos-n], w.buffer[n:w.writePos])
		}
		// 更新写入位置，减去已写入的字节数
		w.writePos -= n
		// 记录写入错误
		w.writeError = err
		// 返回写入错误
		return err
	}

	// 如果写入成功，清空缓冲区，重置写入位置
	w.writePos = 0
	return nil
}

// Available 返回缓冲区中剩余可用的字节数
func (w *BufferedWriter) Available() int { return len(w.buffer) - w.writePos }

// Buffered 返回缓冲区中已写入但未被 Flush 的字节数
func (w *BufferedWriter) Buffered() int { return w.writePos }

// Write 将 data 的内容写入缓冲区
func (w *BufferedWriter) Write(data []byte) (nn int, err error) {
	// 当要写入的数据大于缓冲区的剩余空间，且之前没有发生写入错误时进入循环
	for len(data) > w.Available() && w.writeError == nil {
		var n int
		if w.Buffered() == 0 {
			// 如果缓冲区为空，直接将大块数据写入底层的 writer
			n, w.writeError = w.underlyingWriter.Write(data)
		} else {
			// 否则，将部分数据写入缓冲区
			n = copy(w.buffer[w.writePos:], data)
			// 更新缓冲区写入位置
			w.writePos += n
			// flush 将缓冲区写入底层 writer
			w.flush()
		}
		// 记录已经成功写入的字节数
		nn += n
		// 更新剩余待写入的数据
		data = data[n:]
	}

	// 如果写入过程中发生了错误，直接返回已经写入的字节数和错误
	if w.writeError != nil {
		return nn, w.writeError
	}

	// 如果有剩余数据，先尽可能地写入到缓冲区中，等缓冲区满了再通过 flush() 写入底层
	// 在处理完大于缓冲区剩余空间的数据后，将剩余的较小部分数据写入到缓冲区中
	n := copy(w.buffer[w.writePos:], data)
	// 更新缓冲区写入位置
	w.writePos += n
	// 记录已经成功写入的字节数
	nn += n

	// 返回成功写入的字节数和 nil 错误
	return nn, nil
}

// WriteRaw 如果缓冲区为空，将 data 的内容直接写入底层 BufferedWriter，跳过缓冲区
func (w *BufferedWriter) WriteRaw(data []byte) (nn int, err error) {
	if w.writeError != nil {
		return 0, w.writeError
	}
	if w.Buffered() == 0 {
		// 缓冲区为空，直接写入底层 BufferedWriter
		nn, err = w.underlyingWriter.Write(data)
		w.writeError = err
	} else {
		nn, err = w.Write(data)
	}
	return
}

// Peek 返回接下来的 n 个字节，并推进 writePos，返回这些字节给外部使用。
func (w *BufferedWriter) Peek(n int) ([]byte, error) {
	// 如果传入的 n 是负数，返回错误 ErrNegativeCount
	if n < 0 {
		return nil, ErrNegativeCount
	}

	// 如果请求的字节数 n 大于缓冲区的长度，返回错误 ErrBufferFull
	if n > len(w.buffer) {
		return nil, ErrBufferFull
	}

	// 当缓冲区可用的数据小于请求的字节数 n 并且没有发生写入错误时，持续刷新缓冲区
	for w.Available() < n && w.writeError == nil {
		// 刷新缓冲区，试图腾出空间或写出数据
		w.flush()
	}

	// 如果在刷新过程中发生了写入错误，返回该错误
	if w.writeError != nil {
		return nil, w.writeError
	}

	// 从缓冲区中获取接下来的 n 个字节
	data := w.buffer[w.writePos : w.writePos+n]
	// 更新写入位置的指针
	w.writePos += n

	// 返回读取到的数据和 nil 错误
	return data, nil
}

// WriteString 将字符串 s 写入缓冲区
func (w *BufferedWriter) WriteString(s string) (int, error) {
	nn := 0
	// s 所需的字节数大于可用长度，for 循环 flush 清空持续写入
	for len(s) > w.Available() && w.writeError == nil {
		n := copy(w.buffer[w.writePos:], s)
		w.writePos += n
		nn += n
		s = s[n:]
		w.flush()
	}
	if w.writeError != nil {
		return nn, w.writeError
	}
	// s 剩余部分写入缓冲区
	n := copy(w.buffer[w.writePos:], s)
	w.writePos += n
	nn += n
	return nn, nil
}
