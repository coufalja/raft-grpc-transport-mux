package transport

import (
	"context"
	"io"

	pb "github.com/coufalja/raft-grpc-transport-mux/proto"

	"github.com/hashicorp/raft"
)

// These are requests incoming over gRPC that we need to relay to the Raft engine.

type gRPCAPI struct {
	manager *Manager

	// "Unsafe" to ensure compilation fails if new methods are added but not implemented
	pb.UnsafeRaftTransportServer
}

func (g gRPCAPI) handleRPC(group string, command raft.WithRPCHeader, data io.Reader) (interface{}, error) {
	ch := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  command,
		RespChan: ch,
		Reader:   data,
	}

	var rpcChan chan raft.RPC
	var heartbeatFunc func(rpc raft.RPC)
	if h, ok := g.manager.groups[group]; ok {
		heartbeatFunc = h.heartbeatFunc
		rpcChan = h.rpcChan
	}
	if isHeartbeat(command) {
		// We can take the fast path and use the heartbeat callback and skip the queue in g.manager.rpcChan.
		if heartbeatFunc != nil {
			heartbeatFunc(rpc)
			goto wait
		}
	}
	rpcChan <- rpc
wait:
	resp := <-ch
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Response, nil
}

func (g gRPCAPI) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	group, r := decodeAppendEntriesRequest(req)
	resp, err := g.handleRPC(group, r, nil)
	if err != nil {
		return nil, err
	}
	return encodeAppendEntriesResponse(group, resp.(*raft.AppendEntriesResponse)), nil
}

func (g gRPCAPI) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	group, r := decodeRequestVoteRequest(req)
	resp, err := g.handleRPC(group, r, nil)
	if err != nil {
		return nil, err
	}
	return encodeRequestVoteResponse(group, resp.(*raft.RequestVoteResponse)), nil
}

func (g gRPCAPI) TimeoutNow(ctx context.Context, req *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	group, r := decodeTimeoutNowRequest(req)
	resp, err := g.handleRPC(group, r, nil)
	if err != nil {
		return nil, err
	}
	return encodeTimeoutNowResponse(group, resp.(*raft.TimeoutNowResponse)), nil
}

func (g gRPCAPI) InstallSnapshot(s pb.RaftTransport_InstallSnapshotServer) error {
	isr, err := s.Recv()
	if err != nil {
		return err
	}
	group, r := decodeInstallSnapshotRequest(isr)
	resp, err := g.handleRPC(group, r, &snapshotStream{s, isr.GetData()})
	if err != nil {
		return err
	}
	return s.SendAndClose(encodeInstallSnapshotResponse(group, resp.(*raft.InstallSnapshotResponse)))
}

type snapshotStream struct {
	s pb.RaftTransport_InstallSnapshotServer

	buf []byte
}

func (s *snapshotStream) Read(b []byte) (int, error) {
	if len(s.buf) > 0 {
		n := copy(b, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}
	m, err := s.s.Recv()
	if err != nil {
		return 0, err
	}
	n := copy(b, m.GetData())
	if n < len(m.GetData()) {
		s.buf = m.GetData()[n:]
	}
	return n, nil
}

func (g gRPCAPI) AppendEntriesPipeline(s pb.RaftTransport_AppendEntriesPipelineServer) error {
	for {
		msg, err := s.Recv()
		if err != nil {
			return err
		}
		group, r := decodeAppendEntriesRequest(msg)
		resp, err := g.handleRPC(group, r, nil)
		if err != nil {
			// TODO(quis): One failure doesn't have to break the entire stream?
			// Or does it all go wrong when it's out of order anyway?
			return err
		}
		if err := s.Send(encodeAppendEntriesResponse(group, resp.(*raft.AppendEntriesResponse))); err != nil {
			return err
		}
	}
}

func isHeartbeat(command interface{}) bool {
	req, ok := command.(*raft.AppendEntriesRequest)
	if !ok {
		return false
	}
	return req.Term != 0 && len(req.Leader) != 0 && req.PrevLogEntry == 0 && req.PrevLogTerm == 0 && len(req.Entries) == 0 && req.LeaderCommitIndex == 0
}
