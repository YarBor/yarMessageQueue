package MessageMem

import (
	"sync"
	"sync/atomic"
)

var blockEntryNums = 16

type Block struct {
	mu   sync.RWMutex
	Data [][]byte
	Nums int64
	Size int64
}

func newBlock() *Block {
	return &Block{
		Data: make([][]byte, 0, blockEntryNums),
		Nums: 0,
		Size: 0,
	}
}

type EntryBlocks struct {
	mu           sync.RWMutex
	Ens          []*Block
	BeginOffset  int64
	EntryMaxSize int64 // max size of all entries
}

// Safe
func (ebs *EntryBlocks) LoseEarliestBlock() *Block {
	ebs.mu.Lock()
	defer ebs.mu.Unlock()
	if len(ebs.Ens) <= 1 {
		return nil
	} else {
		tgt := append(make([]*Block, 0, len(ebs.Ens)-1), ebs.Ens[1:]...)
		res := ebs.Ens[0]
		ebs.BeginOffset += ebs.Ens[0].Nums
		ebs.Ens = tgt
		return res
	}
}

func (b *Block) getSize() int64 {
	return b.Size
}

func (b *Block) getNums() int64 {
	return b.Nums
}

func (b *Block) write(entry []byte) {
	b.Data = append(b.Data, entry)
	b.Nums++
	b.Size += int64(len(entry))
}
func (b *Block) read(needReadEntBegin, entNum int64) ([][]byte, int64, int64) {
	var res [][]byte
	if needReadEntBegin > b.Nums {
		return nil, 0, b.Nums
	} else {
		if needReadEntBegin+entNum > b.Nums {
			res = b.Data[needReadEntBegin-1:]
		} else {
			res = b.Data[needReadEntBegin-1 : needReadEntBegin+entNum]
		}
	}
	return res, int64(len(res)), 0
}

func (ebs *EntryBlocks) GetMaxSize() int64 {
	return ebs.EntryMaxSize
}

func (ebs *EntryBlocks) Write(entry []byte) {
	ebs.mu.Lock()
	last := ebs.Ens[len(ebs.Ens)-1]

	last.mu.RLock()
	size := last.getSize()
	last.mu.RUnlock()

	if size >= ebs.GetMaxSize() {
		last = newBlock()
		ebs.Ens = append(ebs.Ens, last)
	}

	last.mu.Lock()
	last.write(entry)
	last.mu.Unlock()
	ebs.mu.Unlock()
}

func (ebs *EntryBlocks) read(EntryBegin, entNum int64) ([][]byte, int64) {
	res := make([][]byte, 0, entNum)
	var readVal [][]byte
	var readNum int64
	var jumpNum int64
	for i := 0; i < len(ebs.Ens); i++ {
		ebs.Ens[i].mu.RLock()
		readVal, readNum, jumpNum = ebs.Ens[i].read(EntryBegin, entNum)
		ebs.Ens[i].mu.RUnlock()
		if jumpNum != 0 {
			EntryBegin -= jumpNum
		} else {
			if readNum == entNum {
				res = readVal
			} else {
				res = append(res, readVal...)
			}
			if int64(len(res)) == entNum {
				break
			}
			entNum -= readNum
		}
	}
	return res, int64(len(res))
}

func (ebs *EntryBlocks) Read(offset, num int64) (res [][]byte, readEntriesNumb int64, NowOffset int64) {
	ebs.mu.RLock()
	defer ebs.mu.RUnlock()
	if num <= 0 {
		panic("EntryBlocks get negative number")
	}
	if offset < ebs.BeginOffset {
		offset = ebs.BeginOffset
	}
	res, readEntriesNumb = ebs.read(offset, num)
	return res, readEntriesNumb, offset + readEntriesNumb
}

type MessageEntry struct {
	En   EntryBlocks
	mode int32 //0 normal , 1 Losing

	MaxEntries uint64
	MaxSize    uint64

	EntriesNow uint64
	SizeNow    uint64
}

func (me *MessageEntry) Write(bt []byte) {
	me.En.Write(bt)
	atomic.AddUint64(&me.EntriesNow, 1)
	atomic.AddUint64(&me.SizeNow, uint64(len(bt)))

	if atomic.LoadUint64(&me.EntriesNow) >= me.MaxEntries || atomic.LoadUint64(&me.SizeNow) >= me.MaxSize {
		if atomic.CompareAndSwapInt32(&me.mode, 0, 1) {
			go me.LoseLastOne()
		}
	}
}

func (me *MessageEntry) Read(offset, num int64) ([][]byte, int64) {
	return me.En.read(offset, num)
}

func (me *MessageEntry) LoseLastOne() {
	defer atomic.StoreInt32(&me.mode, 0)

	bk := me.En.LoseEarliestBlock()
	if bk == nil {
		return
	}
	atomic.AddUint64(&me.EntriesNow, uint64(bk.Nums))
	atomic.AddUint64(&me.SizeNow, uint64(bk.Size))
}
func NewMessageEntry(MaxEntries, MaxSize uint64) *MessageEntry {
	return &MessageEntry{
		En: EntryBlocks{
			Ens:          append(make([]*Block, 0), newBlock()),
			BeginOffset:  0,
			EntryMaxSize: 0,
		},
		mode:       0,
		MaxEntries: MaxEntries,
		MaxSize:    MaxSize,
		EntriesNow: 0,
		SizeNow:    0,
	}
}

func (me *MessageEntry) Handle(command interface{}) error {
	bt, ok := command.([]byte)
	if !ok {
		panic("Invalid MessageEntry command")
	}
	me.En.Write(bt)
	return nil
}

// TODO:

func (me *MessageEntry) MakeSnapshot() []byte {
	return nil
}

func (me *MessageEntry) LoadSnapshot([]byte) {

}
