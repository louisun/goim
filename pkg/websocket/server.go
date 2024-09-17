package websocket

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"github.com/Terry-Mao/goim/pkg/bufio"
)

var (
	keyGUID = []byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
	// ErrBadRequestMethod bad request method
	ErrBadRequestMethod = errors.New("bad method")
	// ErrNotWebSocket not websocket protocal
	ErrNotWebSocket = errors.New("not websocket protocol")
	// ErrBadWebSocketVersion bad websocket version
	ErrBadWebSocketVersion = errors.New("missing or bad WebSocket Version")
	// ErrChallengeResponse mismatch challenge response
	ErrChallengeResponse = errors.New("mismatch challenge/response")
)

// Upgrade 处理 WebSocket 协议升级
func Upgrade(rwc io.ReadWriteCloser, bufferedReader *bufio.BufferedReader, bufferedWriter *bufio.BufferedWriter, request *Request) (*Conn, error) {
	// 校验是否为 GET 方法
	if request.Method != "GET" {
		return nil, ErrBadRequestMethod
	}

	// 校验 WebSocket 版本是否为 13
	if request.Header.Get("Sec-Websocket-Version") != "13" {
		return nil, ErrBadWebSocketVersion
	}

	// 校验 Upgrade 头是否为 "websocket"
	if strings.ToLower(request.Header.Get("Upgrade")) != "websocket" {
		return nil, ErrNotWebSocket
	}

	// 校验 Connection 头中是否包含 "upgrade"
	if !strings.Contains(strings.ToLower(request.Header.Get("Connection")), "upgrade") {
		return nil, ErrNotWebSocket
	}

	// 获取客户端的 WebSocket 握手密钥
	challengeKey := request.Header.Get("Sec-Websocket-Key")
	if challengeKey == "" {
		return nil, ErrChallengeResponse
	}

	// 构建 WebSocket 握手响应头
	_, _ = bufferedWriter.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	_, _ = bufferedWriter.WriteString("Upgrade: websocket\r\n")
	_, _ = bufferedWriter.WriteString("Connection: Upgrade\r\n")
	_, _ = bufferedWriter.WriteString("Sec-WebSocket-Accept: " + computeAcceptKey(challengeKey) + "\r\n\r\n")

	// 刷新写缓冲区，发送响应
	if err := bufferedWriter.Flush(); err != nil {
		return nil, err
	}

	// 返回新建立的 WebSocket 连接
	return newConn(rwc, bufferedReader, bufferedWriter), nil
}

func computeAcceptKey(challengeKey string) string {
	h := sha1.New()
	_, _ = h.Write([]byte(challengeKey))
	_, _ = h.Write(keyGUID)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
