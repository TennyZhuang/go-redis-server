// go-redis-server is a helper library for building server software capable of speaking the redis protocol.
// This could be an alternate implementation of redis, a custom proxy to redis,
// or even a completely different backend capable of "masquerading" its API as a redis database.

package redis

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"reflect"

	"golang.org/x/net/netutil"
)

type Server struct {
	Proto        string
	Addr         string // TCP address to listen on, ":6389" if empty
	MonitorChans []chan string
	methods      map[string]HandlerFn
	onConnect    func()
	onDisconnect func()
}

// ListenAndServe receives an argument maxConnection, which limit max connection it can accept simultaneous, passing maxConnection <= 0 means no limit.
func (srv *Server) ListenAndServe(maxConnection int) error {
	addr := srv.Addr
	if srv.Proto == "" {
		srv.Proto = "tcp"
	}
	if srv.Proto == "unix" && addr == "" {
		addr = "/tmp/redis.sock"
	} else if addr == "" {
		addr = ":6389"
	}
	l, e := net.Listen(srv.Proto, addr)
	if e != nil {
		return e
	}
	if maxConnection > 0 {
		l = netutil.LimitListener(l, maxConnection)
	}
	return srv.Serve(l)
}

// Serve accepts incoming connections on the Listener l, creating a
// new service goroutine for each.  The service goroutines read requests and
// then call srv.Handler to reply to them.
func (srv *Server) Serve(l net.Listener) error {
	defer l.Close()
	srv.MonitorChans = []chan string{}
	for {
		rw, err := l.Accept()
		if err != nil {
			return err
		}
		go srv.ServeClient(rw)
	}
}

// Serve starts a new redis session, using `conn` as a transport.
// It reads commands using the redis protocol, passes them to `handler`,
// and returns the result.
func (srv *Server) ServeClient(conn net.Conn) (err error) {
	defer func() {
		if srv.onDisconnect != nil {
			srv.onDisconnect()
		}
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
		conn.Close()
	}()

	clientChan := make(chan struct{})

	// Read on `conn` in order to detect client disconnect
	go func() {
		// Close chan in order to trigger eventual selects
		defer close(clientChan)
		defer Debugf("Client disconnected")
		// FIXME: move conn within the request.
		if false {
			io.Copy(ioutil.Discard, conn)
		}
	}()

	var clientAddr string

	switch co := conn.(type) {
	case *net.UnixConn:
		f, err := conn.(*net.UnixConn).File()
		if err != nil {
			return err
		}
		clientAddr = f.Name()
	default:
		clientAddr = co.RemoteAddr().String()
	}

	if srv.onConnect != nil {
		srv.onConnect()
	}

	br := bufio.NewReader(conn)
	for {
		request, err := parseRequest(br)
		if err != nil {
			return err
		}
		request.Host = clientAddr
		request.ClientChan = clientChan
		reply, err := srv.Apply(request)
		if err != nil {
			return err
		}
		if _, err = reply.WriteTo(conn); err != nil {
			return err
		}
	}
	return nil
}

func NewServer(c *Config) (*Server, error) {
	srv := &Server{
		Proto:        c.proto,
		MonitorChans: []chan string{},
		methods:      make(map[string]HandlerFn),
	}

	if srv.Proto == "unix" {
		srv.Addr = c.host
	} else {
		srv.Addr = fmt.Sprintf("%s:%d", c.host, c.port)
	}

	if c.handler == nil {
		c.handler = NewDefaultHandler()
	}

	rh := reflect.TypeOf(c.handler)

	onConnect, ok := rh.MethodByName("OnConnect")
	if ok {
		srv.onConnect = func() {
			onConnect.Func.Call([]reflect.Value{reflect.ValueOf(c.handler)})
		}
	}

	onDisconnect, ok := rh.MethodByName("OnDisconnect")
	if ok {
		srv.onDisconnect = func() {
			onDisconnect.Func.Call([]reflect.Value{reflect.ValueOf(c.handler)})
		}
	}
	for i := 0; i < rh.NumMethod(); i++ {
		method := rh.Method(i)
		if (method.Name[0] > 'a' && method.Name[0] < 'z') || method.Name == "OnConnect" || method.Name == "OnDisconnect" {
			continue
		}
		println(method.Name)
		handlerFn, err := srv.createHandlerFn(c.handler, &method.Func)
		if err != nil {
			return nil, err
		}
		srv.Register(method.Name, handlerFn)
	}
	return srv, nil
}
