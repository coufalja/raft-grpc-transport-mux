package transport

import (
	pb "github.com/coufalja/raft-grpc-transport-mux/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func encodeAppendEntriesRequest(group string, s *raft.AppendEntriesRequest) *pb.AppendEntriesRequest {
	return &pb.AppendEntriesRequest{
		RpcHeader:         encodeRPCHeader(group, s.RPCHeader),
		Term:              s.Term,
		Leader:            s.Leader,
		PrevLogEntry:      s.PrevLogEntry,
		PrevLogTerm:       s.PrevLogTerm,
		Entries:           encodeLogs(s.Entries),
		LeaderCommitIndex: s.LeaderCommitIndex,
	}
}

func encodeRPCHeader(group string, s raft.RPCHeader) *pb.RPCHeader {
	return &pb.RPCHeader{
		ProtocolVersion: int64(s.ProtocolVersion),
		Id:              s.ID,
		Addr:            s.Addr,
		Group:           []byte(group),
	}
}

func encodeLogs(s []*raft.Log) []*pb.Log {
	ret := make([]*pb.Log, len(s))
	for i, l := range s {
		ret[i] = encodeLog(l)
	}
	return ret
}

func encodeLog(s *raft.Log) *pb.Log {
	return &pb.Log{
		Index:      s.Index,
		Term:       s.Term,
		Type:       encodeLogType(s.Type),
		Data:       s.Data,
		Extensions: s.Extensions,
		AppendedAt: timestamppb.New(s.AppendedAt),
	}
}

func encodeLogType(s raft.LogType) pb.Log_LogType {
	switch s {
	case raft.LogCommand:
		return pb.Log_LOG_COMMAND
	case raft.LogNoop:
		return pb.Log_LOG_NOOP
	case raft.LogAddPeerDeprecated:
		return pb.Log_LOG_ADD_PEER_DEPRECATED
	case raft.LogRemovePeerDeprecated:
		return pb.Log_LOG_REMOVE_PEER_DEPRECATED
	case raft.LogBarrier:
		return pb.Log_LOG_BARRIER
	case raft.LogConfiguration:
		return pb.Log_LOG_CONFIGURATION
	default:
		panic("invalid LogType")
	}
}

func encodeAppendEntriesResponse(group string, s *raft.AppendEntriesResponse) *pb.AppendEntriesResponse {
	return &pb.AppendEntriesResponse{
		RpcHeader:      encodeRPCHeader(group, s.RPCHeader),
		Term:           s.Term,
		LastLog:        s.LastLog,
		Success:        s.Success,
		NoRetryBackoff: s.NoRetryBackoff,
	}
}

func encodeRequestVoteRequest(group string, s *raft.RequestVoteRequest) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		RpcHeader:          encodeRPCHeader(group, s.RPCHeader),
		Term:               s.Term,
		Candidate:          s.Candidate,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		LeadershipTransfer: s.LeadershipTransfer,
	}
}

func encodeRequestVoteResponse(group string, s *raft.RequestVoteResponse) *pb.RequestVoteResponse {
	return &pb.RequestVoteResponse{
		RpcHeader: encodeRPCHeader(group, s.RPCHeader),
		Term:      s.Term,
		Peers:     s.Peers,
		Granted:   s.Granted,
	}
}

func encodeInstallSnapshotRequest(group string, s *raft.InstallSnapshotRequest) *pb.InstallSnapshotRequest {
	return &pb.InstallSnapshotRequest{
		RpcHeader:          encodeRPCHeader(group, s.RPCHeader),
		SnapshotVersion:    int64(s.SnapshotVersion),
		Term:               s.Term,
		Leader:             s.Leader,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		Peers:              s.Peers,
		Configuration:      s.Configuration,
		ConfigurationIndex: s.ConfigurationIndex,
		Size:               s.Size,
	}
}

func encodeInstallSnapshotResponse(group string, s *raft.InstallSnapshotResponse) *pb.InstallSnapshotResponse {
	return &pb.InstallSnapshotResponse{
		RpcHeader: encodeRPCHeader(group, s.RPCHeader),
		Term:      s.Term,
		Success:   s.Success,
	}
}

func encodeTimeoutNowRequest(group string, s *raft.TimeoutNowRequest) *pb.TimeoutNowRequest {
	return &pb.TimeoutNowRequest{
		RpcHeader: encodeRPCHeader(group, s.RPCHeader),
	}
}

func encodeTimeoutNowResponse(group string, s *raft.TimeoutNowResponse) *pb.TimeoutNowResponse {
	return &pb.TimeoutNowResponse{
		RpcHeader: encodeRPCHeader(group, s.RPCHeader),
	}
}
