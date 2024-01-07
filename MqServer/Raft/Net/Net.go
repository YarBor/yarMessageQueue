package Net

import (
	"MqServer/Log"
	"MqServer/Raft"
	"MqServer/Raft/Gob"
	pb "MqServer/rpc"
	"bytes"
	"context"
	"errors"
	"google.golang.org/grpc"
)

type ClientEnd struct {
	pb.RaftCallClient
	Rfn  *Raft.RaftNode // father
	Conn *grpc.ClientConn
}

// *RequestArgs *RequestReply
func (c *ClientEnd) Call(fName string, args, reply interface{}) bool {
	var err error
	arg, ok := args.(*Raft.RequestArgs)
	if !ok {
		panic("args translate error")
	}
	rpl, ok := reply.(*Raft.RequestReply)
	if !ok {
		panic("reply translate error")
	}
	buff := bytes.Buffer{}
	if err = Gob.NewEncoder(&buff).Encode(*arg); err != nil {
		panic("encode error")
	}

	switch fName {
	case "Raft.RequestVote":
		i, err := c.RequestVote(context.Background(), &pb.RequestVoteRequest{
			Topic:     c.Rfn.T,
			Partition: c.Rfn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			Log.ERROR(err.Error())
			return false
		} else if err != nil {
			Log.ERROR(err.Error())
			return false
		}
		if err = Gob.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
		break
	case "Raft.RequestPreVote":
		i, err := c.RequestPreVote(context.Background(), &pb.RequestPreVoteRequest{
			Topic:     c.Rfn.T,
			Partition: c.Rfn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			Log.ERROR(err.Error())
			return false
		} else if err != nil {
			Log.ERROR(err.Error())
			return false
		}
		if err = Gob.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
		break
	case "Raft.HeartBeat":
		i, err := c.HeartBeat(context.Background(), &pb.HeartBeatRequest{
			Topic:     c.Rfn.T,
			Partition: c.Rfn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			Log.ERROR(err.Error())
			return false
		} else if err != nil {
			Log.ERROR(err.Error())
			return false
		}
		if err = Gob.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
		break
	default:
		panic("unknown RPC request")
	}
	return true
}
