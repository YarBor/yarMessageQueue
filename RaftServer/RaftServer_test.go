package RaftServer

import (
	MyLog "MqServer/Log"
	"testing"
	"time"
)

func TestMakeRaftServer(t *testing.T) {
	ser, err := MakeRaftServer()
	if err != nil {
		panic(err)
	}
	ser.SetRaftServerInfo("1", "127.0.0.1:20000")
	go func() {
		err = ser.Serve()
		if err != nil {
			panic("Server fail")
		}
	}()
	time.Sleep(3 * time.Second)
	ser.Stop()
}

func make3RaftServers(ip ...struct{ Url, ID string }) (ser1 *RaftServer, ser2 *RaftServer, ser3 *RaftServer) {
	var err error
	ser1, err = MakeRaftServer()
	if err != nil {
		panic("Server fail")
		return
	}
	ser1.SetRaftServerInfo(ip[0].ID, ip[0].Url)
	go func() {
		err = ser1.Serve()
		if err != nil {
			panic(err)
		}
	}()
	ser2, err = MakeRaftServer()
	if err != nil {
		panic("Server fail")
	}
	ser2.SetRaftServerInfo(ip[1].ID, ip[1].Url)
	go func() {
		err = ser2.Serve()
		if err != nil {
			panic(err)
		}
	}()
	ser3, err = MakeRaftServer()
	if err != nil {
		panic("Server fail")
	}
	ser3.SetRaftServerInfo(ip[2].ID, ip[2].Url)
	go func() {
		err = ser3.Serve()
		if err != nil {
			panic(err)
		}
	}()
	return
}

type testHandle struct {
}

func (s *testHandle) Handle(i interface{}) (error, interface{}) {
	MyLog.DEBUG("testHandle handle")
	return nil, i
}

func (s *testHandle) MakeSnapshot() []byte {
	MyLog.DEBUG("MakeSnapshot")
	return []byte("nihao")
}

func (s *testHandle) LoadSnapshot(bytes []byte) {
	MyLog.DEBUG("LoadSnapshot")
}

func TestMakeRaftServer_1(t *testing.T) {
	err := MyLog.SetLogLevel(MyLog.LogLevel_TRACE)
	if err != nil {
		panic(err)
	}
	data := []struct{ Url, ID string }{
		{"127.0.0.1:10000", "0"},
		{"127.0.0.1:10001", "1"},
		{"127.0.0.1:10002", "2"},
	}
	s1, s2, s3 := make3RaftServers(data...)
	t1 := testHandle{}
	t2 := testHandle{}
	t3 := testHandle{}
	node1, err1 := s1.RegisterMetadataRaft(data, &t1, &t1)
	if err1 != nil {
		panic(err1)
	} else {
		node1.Start()
	}
	node2, err2 := s2.RegisterMetadataRaft(data, &t2, &t2)
	if err2 != nil {
		panic(err2)
	} else {
		node2.Start()
	}
	node3, err3 := s3.RegisterMetadataRaft(data, &t3, &t3)
	if err3 != nil {
		panic(err3)
	} else {
		node3.Start()
	}
	println(node1)
	println(node2)
	println(node3)
	time.Sleep(time.Second * 10)
	s1.Stop()
	s2.Stop()
	s3.Stop()
}
