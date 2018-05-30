package redis

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type mockHandler struct {
	cnt int32
}

func (h *mockHandler) OnConnect() {
	atomic.AddInt32(&h.cnt, 1)
}

func (h *mockHandler) OnDisconnect() {
	atomic.AddInt32(&h.cnt, -1)
}

func TestServer(t *testing.T) {
	handler := &mockHandler{}
	cfg := DefaultConfig().Handler(handler)
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	server, client := net.Pipe()
	go func() {
		srv.ServeClient(client)
	}()

	time.Sleep(50 * time.Millisecond)
	if handler.cnt != 1 {
		t.Fatal(handler.cnt)
	}

	server.Write([]byte("*InvalidRequest\r\n\r\n\r\n"))
	time.Sleep(50 * time.Millisecond)
	if handler.cnt != 0 {
		t.Fatal(handler.cnt)
	}

	server.Close()
}
