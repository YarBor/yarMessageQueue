package raft_server

import (
	"bytes"
	"context"
	"errors"
)

import (
	"google.golang.org/grpc"
)

import (
	pb "github.com/YarBor/BorsMqServer/api"
	PgLog "github.com/YarBor/BorsMqServer/logger"
	"github.com/YarBor/BorsMqServer/raft_server/pack"
)

type ClientEnd struct {
	pb.RaftCallClient
	ID   string
	rfn  *RaftNode // father
	Conn *grpc.ClientConn
}

// *RequestArgs *RequestReply
func (c *ClientEnd) Call(fName string, args, reply interface{}) bool {
	var err error
	arg, ok := args.(*RequestArgs)
	if !ok {
		panic("args translate error")
	}
	rpl, ok := reply.(*RequestReply)
	if !ok {
		panic("reply translate error")
	}
	buff := bytes.Buffer{}
	if err = pack.NewEncoder(&buff).Encode(*arg); err != nil {
		panic("encode error")
	}

	switch fName {
	case "RaftServer.RequestVote":
		i, err := c.RequestVote(context.Background(), &pb.RequestVoteRequest{
			Topic:     c.rfn.T,
			Partition: c.rfn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			PgLog.ERROR(err.Error(), *c)
			return false
		} else if err != nil {
			PgLog.ERROR(err.Error(), *c)
			return false
		}
		if err = pack.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
	case "RaftServer.RequestPreVote":
		i, err := c.RequestPreVote(context.Background(), &pb.RequestPreVoteRequest{
			Topic:     c.rfn.T,
			Partition: c.rfn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			PgLog.ERROR(err.Error(), *c)
			return false
		} else if err != nil {
			PgLog.ERROR(err.Error(), *c)
			return false
		}
		if err = pack.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
	case "RaftServer.HeartBeat":
		i, err := c.HeartBeat(context.Background(), &pb.HeartBeatRequest{
			Topic:     c.rfn.T,
			Partition: c.rfn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			PgLog.ERROR(err.Error(), *c)
			return false
		} else if err != nil {
			PgLog.ERROR(err.Error(), *c)
			return false
		}
		if err = pack.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
	default:
		panic("unknown RPC request")
	}
	return true
}
