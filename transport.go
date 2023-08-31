// Package transport provides Transport for github.com/hashicorp/raft over gRPC.
package transport

import (
	"errors"
	"sync"
	"time"

	pb "github.com/coufalja/raft-grpc-transport-mux/proto"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

var (
	errCloseErr = errors.New("error closing connections")
)

type Manager struct {
	localAddress raft.ServerAddress
	dialOptions  []grpc.DialOption

	heartbeatTimeout time.Duration

	connectionsMtx sync.Mutex
	connections    map[raft.ServerID]*conn

	groups map[string]*raftAPI
}

// New creates both components of raft-grpc-transport: a gRPC service and a Raft Transport.
func New(localAddress raft.ServerAddress, dialOptions []grpc.DialOption, options ...Option) *Manager {
	m := &Manager{
		localAddress: localAddress,
		dialOptions:  dialOptions,
		connections:  make(map[raft.ServerID]*conn),
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

// Register the RaftTransport gRPC service on a gRPC server.
func (m *Manager) Register(s *grpc.Server) {
	pb.RegisterRaftTransportServer(s, gRPCAPI{manager: m})
}

// Transport returns a raft.Transport that communicates over gRPC. Group string scopes the communication under this string label.
// That allows for reusing Transport manager (and its connections) by multiple `raft.Raft` instances.
func (m *Manager) Transport(group string) raft.Transport {
	api := &raftAPI{manager: m, rpcChan: make(chan raft.RPC), group: group}
	if m.groups == nil {
		m.groups = make(map[string]*raftAPI)
	}
	m.groups[group] = api
	return api
}

func (m *Manager) Close() error {
	m.connectionsMtx.Lock()
	defer m.connectionsMtx.Unlock()

	err := errCloseErr
	for _, conn := range m.connections {
		// Lock conn.mtx to ensure Dial() is complete
		conn.mtx.Lock()
		conn.mtx.Unlock()
		closeErr := conn.clientConn.Close()
		if closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}

	if !errors.Is(err, errCloseErr) {
		return err
	}

	return nil
}
