# raft-grpc-transport-mux

[![Godoc](https://godoc.org/github.com/coufalja/raft-grpc-transport-mux?status.svg)](https://godoc.org/github.com/coufalja/raft-grpc-transport-mux)

This library provides a [Transport](https://godoc.org/github.com/hashicorp/raft#Transport)
for https://github.com/hashicorp/raft over gRPC.

One benefit of this is that gRPC is easy to multiplex over a single port. It is an extension of
original https://github.com/Jille/raft-grpc-transport
and an example how to multiplex more Raft groups in the single process.

## Usage

```go
// ...
tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})
s := grpc.NewServer()
tm.Register(s)
r, err := raft.NewRaft(..., tm.Transport("group1"))
// ...
r2, err := raft.NewRaft(..., tm.Transport("group2"))
// ...
```

Want more example code? Check out main.go at https://github.com/Jille/raft-grpc-example
