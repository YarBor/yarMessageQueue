package MessageMem

import (
	"bytes"
	"github.com/YarBor/BorsMqServer/RaftServer/Pack"
	"github.com/YarBor/BorsMqServer/common"
	"sync/atomic"
)

var blockEntryNums = 16

type MessageEntry struct {
	En   EntryBlocks
	mode int32 //0 normal , 1 Losing

	MaxEntries uint64
	MaxSize    uint64

	EntriesStorageNow uint64
	SizeStorageNow    uint64
}

func (e *MessageEntry) LoseLastOne() {
	defer atomic.StoreInt32(&e.mode, 0)

	bk := e.En.LoseEarliestBlock()
	if bk == nil {
		return
	}
	atomic.AddUint64(&e.EntriesStorageNow, uint64(-1*bk.Nums))
	atomic.AddUint64(&e.SizeStorageNow, uint64(-1*bk.Size))
}

func (m *MessageEntry) MakeSnapshot() []byte {
	bf := bytes.NewBuffer(nil)
	encode := Pack.NewEncoder(bf)
	err := encode.Encode(atomic.LoadInt32(&m.mode))
	if err != nil {
		panic(err)
	}
	err = encode.Encode(atomic.LoadUint64(&m.MaxEntries))
	if err != nil {
		panic(err)
	}
	err = encode.Encode(atomic.LoadUint64(&m.MaxSize))
	if err != nil {
		panic(err)
	}
	err = encode.Encode(m.En.MakeSnapshot())
	if err != nil {
		panic(err)
	}
	err = encode.Encode(atomic.LoadUint64(&m.EntriesStorageNow))
	if err != nil {
		panic(err)
	}
	err = encode.Encode(atomic.LoadUint64(&m.SizeStorageNow))
	if err != nil {
		panic(err)
	}
	return bf.Bytes()
}

func (m *MessageEntry) LoadSnapshot(data []byte) {
	bf := bytes.NewBuffer(data)
	decode := Pack.NewDecoder(bf)

	var mode int32
	if err := decode.Decode(&mode); err != nil {
		panic(err)
	}
	atomic.StoreInt32(&m.mode, mode)

	var maxEntries uint64
	if err := decode.Decode(&maxEntries); err != nil {
		panic(err)
	}
	atomic.StoreUint64(&m.MaxEntries, maxEntries)

	var maxSize uint64
	if err := decode.Decode(&maxSize); err != nil {
		panic(err)
	}
	atomic.StoreUint64(&m.MaxSize, maxSize)

	var EnsData = []byte{}
	if err := decode.Decode(&EnsData); err != nil {
		panic(err)
	}
	m.En.LoadSnapshot(EnsData)

	var entriesNow uint64
	if err := decode.Decode(&entriesNow); err != nil {
		panic(err)
	}
	atomic.StoreUint64(&m.EntriesStorageNow, entriesNow)

	var sizeNow uint64
	if err := decode.Decode(&sizeNow); err != nil {
		panic(err)
	}
	atomic.StoreUint64(&m.SizeStorageNow, sizeNow)
}

func (m *MessageEntry) IsClearToDel(off int64) bool {
	//_, num := m.Read(off, 1, -1)
	return m.En.EndOffset == off
}

func (me *MessageEntry) Write(bt []byte) {
	me.En.Write(bt)
	atomic.AddUint64(&me.EntriesStorageNow, 1)
	atomic.AddUint64(&me.SizeStorageNow, uint64(len(bt)))
	if atomic.LoadUint64(&me.EntriesStorageNow) >= me.MaxEntries || atomic.LoadUint64(&me.SizeStorageNow) >= me.MaxSize {
		if atomic.CompareAndSwapInt32(&me.mode, 0, 1) {
			go me.LoseLastOne()
		}
	}
}

// Read
// Input Index, MaxEntries, MaxSize
// Return( BeginOff Data ReadNum )
func (me *MessageEntry) Read(Index int64, MaxEntries, MaxSize int32) (int64, [][]byte, int64) {
	if MaxEntries <= 0 {
		MaxEntries = common.DefaultMaxEntriesOf1Read
	}
	if MaxSize <= 0 {
		MaxSize = common.DefaultMaxSizeOf1Read
	}
	return me.En.read(Index, int64(MaxEntries), int64(MaxSize))
}

func NewMessageEntry(MaxEntries, MaxSize uint64, EntryMaxSizeOf1Block int64) *MessageEntry {
	return &MessageEntry{
		En: EntryBlocks{
			Ens:                   append(make([]*Block, 0), newBlock()),
			BeginOffset:           0,
			EntryMaxSizeOf_1Block: EntryMaxSizeOf1Block,
		},
		mode:              0,
		MaxEntries:        MaxEntries,
		MaxSize:           MaxSize,
		EntriesStorageNow: 0,
		SizeStorageNow:    0,
	}
}

//func (me *MessageEntry) Handle(command interface{}) error {
//	bt, ok := command.([]byte)
//	if !ok {
//		panic("Invalid MessageEntry command")
//	}
//	me.En.Write(bt)
//	return nil
//}

// TODO:

func (me *MessageEntry) GetEndOffset() int64 {
	me.En.mu.RLock()
	defer me.En.mu.RUnlock()
	return me.En.EndOffset
}
func (me *MessageEntry) GetBeginOffset() int64 {
	me.En.mu.RLock()
	defer me.En.mu.RUnlock()
	return me.En.BeginOffset
}

//func (me *MessageEntry) MakeSnapshot() []byte {
//	me.En.mu.RLock()
//	defer me.En.mu.RUnlock()
//
//}
//
//func (me *MessageEntry) LoadSnapshot([]byte) {
//	me.En.mu.Lock()
//	defer me.En.mu.Unlock()
//}
