package message_memory

import (
	"bytes"
	"io"
	"sync"
	"time"
)

import (
	Log "github.com/YarBor/BorsMqServer/logger"
	"github.com/YarBor/BorsMqServer/raft_server/pack"
)

type EntryBlocks struct {
	mu                    sync.RWMutex
	Ens                   []*Block
	BeginOffset           int64
	EndOffset             int64
	EntryMaxSizeOf_1Block int64 // max size of all entries
}

func (ebs *EntryBlocks) MakeSnapshot() []byte {
	ebs.mu.RLock()
	defer ebs.mu.RUnlock()
	bf := bytes.NewBuffer(nil)
	encode := pack.NewEncoder(bf)
	err := encode.Encode(ebs.BeginOffset)
	if err != nil {
		panic(err)
	}
	err = encode.Encode(ebs.EndOffset)
	if err != nil {
		panic(err)
	}
	err = encode.Encode(ebs.EntryMaxSizeOf_1Block)
	if err != nil {
		panic(err)
	}
	for _, en := range ebs.Ens {
		err = encode.Encode(en.MakeSnapshot())
		if err != nil {
			panic(err)
		}
	}
	return bf.Bytes()
}

func (ebs *EntryBlocks) LoadSnapshot(data []byte) {
	ebs.mu.Lock()
	defer ebs.mu.Unlock()
	bf := bytes.NewBuffer(data)
	decode := pack.NewDecoder(bf)
	if err := decode.Decode(&ebs.BeginOffset); err != nil {
		panic(err)
	}
	if err := decode.Decode(&ebs.EndOffset); err != nil {
		panic(err)
	}
	if err := decode.Decode(&ebs.EntryMaxSizeOf_1Block); err != nil {
		panic(err)
	}

	var ens []*Block
	for {
		en := []byte{}
		if err := decode.Decode(&en); err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		} else {
			ens = append(ens, SnapshotToBlock(en))
		}
	}
	ebs.Ens = ens
}

func (ebs *EntryBlocks) GetMaxSize() int64 {
	ebs.mu.RLock()
	defer ebs.mu.RUnlock()
	return ebs.EntryMaxSizeOf_1Block
}

func (ebs *EntryBlocks) Write(entry []byte) {
	ebs.mu.Lock()
	last := ebs.Ens[len(ebs.Ens)-1]

	last.mu.RLock()
	size := last.getSize()
	last.mu.RUnlock()

	if size >= ebs.EntryMaxSizeOf_1Block {
		last = newBlock()
		ebs.Ens = append(ebs.Ens, last)
	}

	last.mu.Lock()
	last.write(entry)
	last.mu.Unlock()
	ebs.EndOffset += 1
	ebs.mu.Unlock()
}
func checkFuncDone(FuncName string) func() {
	/* used to check deadlock questions  */
	t := time.Now().UnixMilli()
	i := make(chan bool, 1)
	i2 := make(chan bool, 1)
	go func() {
		Log.DEBUG(-1, t, FuncName+" GO")
		i2 <- true
		for {
			select {
			case <-time.After(50 * 2 * time.Millisecond):
				Log.DEBUG(-1, "\n", t, "!!!!\t", FuncName+" MayLocked\n")
			case <-i:
				close(i)
				return
			}
		}
	}()
	<-i2
	close(i2)
	return func() {
		i <- true
		Log.DEBUG(-1, t, FuncName+" return", time.Now().UnixMilli()-t, "ms")
	}
}
func (ebs *EntryBlocks) read(EntryBegin, MaxEntries, MaxSize int64) (int64, [][]byte, int64) {
	res := make([][]byte, 0)
	var readVal [][]byte
	beginOffset := int64(0)
	ebs.mu.RLock()
	if endOff := ebs.EndOffset; endOff < EntryBegin {
		Log.DEBUG("asdlfkjsadkfljkladfjsafsjkldkldsjfkdfj\n")
		ebs.mu.RUnlock()
		return endOff, nil, 0
	} else if beginOffset = ebs.BeginOffset; beginOffset > EntryBegin {
		EntryBegin = beginOffset
	}
	blks := ebs.Ens[:]
	ebs.mu.RUnlock()
	tmpOff := EntryBegin - beginOffset
	var readNum, jumpNum, ReadSize, ReadSizeAll int64
	for i := 0; i < len(blks); i++ {
		blks[i].mu.RLock()
		readVal, readNum, jumpNum, ReadSize = blks[i].read(tmpOff, MaxEntries)
		blks[i].mu.RUnlock()
		if jumpNum != 0 {
			tmpOff -= jumpNum
		} else if MaxSize < ReadSizeAll {
			break
		} else {
			ReadSizeAll += ReadSize
			if readNum == MaxEntries {
				res = readVal
			} else if readNum > MaxEntries {
				res = append(res, readVal[:MaxEntries]...)
				break
			} else {
				res = append(res, readVal...)
			}
			if int64(len(res)) == MaxEntries {
				break
			}
			MaxEntries -= readNum
		}
	}
	return EntryBegin, res, int64(len(res))
}

func (ebs *EntryBlocks) Read(offset, maxEntries, maxSize int64) (
	res [][]byte, readEntriesNumb int64, NowOffset int64) {
	ebs.mu.RLock()
	defer ebs.mu.RUnlock()
	if maxEntries <= 0 {
		panic("EntryBlocks get negative number")
	}
	if offset < ebs.BeginOffset {
		offset = ebs.BeginOffset
	}
	NowOffset, res, readEntriesNumb = ebs.read(offset, maxEntries, maxSize)
	return res, readEntriesNumb, NowOffset
}
