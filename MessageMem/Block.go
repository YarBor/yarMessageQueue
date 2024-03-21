package MessageMem

import (
	"BorsMqServer/RaftServer/Pack"
	"bytes"
	"sync"
)

type Block struct {
	mu   sync.RWMutex
	Data [][]byte
	Nums int64
	Size int64
}

func (b *Block) MakeSnapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()
	bt := bytes.NewBuffer(nil)
	encode := Pack.NewEncoder(bt)
	err := encode.Encode(b.Data)
	if err != nil {
		panic(err)
	}
	err = encode.Encode(b.Nums)
	if err != nil {
		panic(err)
	}
	err = encode.Encode(b.Size)
	if err != nil {
		panic(err)
	}
	return bt.Bytes()
}

func SnapshotToBlock(bts []byte) *Block {
	b := &Block{}
	bf := bytes.NewBuffer(bts)
	decode := Pack.NewDecoder(bf)
	err := decode.Decode(&b.Data)
	if err != nil {
		panic(err)
	}
	err = decode.Decode(&b.Nums)
	if err != nil {
		panic(err)
	}
	err = decode.Decode(&b.Size)
	if err != nil {
		panic(err)
	}
	return b
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

func (b *Block) read(needReadEntBegin, entNum int64) (
	[][]byte /*data*/, int64 /*readNum*/, int64 /*jumpNum*/, int64 /*ReadSize*/) {
	var res [][]byte
	if needReadEntBegin >= b.Nums {
		return nil, 0, b.Nums, 0
	} else {
		if needReadEntBegin+entNum >= b.Nums {
			res = b.Data[needReadEntBegin:]
		} else {
			res = b.Data[needReadEntBegin : needReadEntBegin+entNum]
		}
	}
	Size := int64(0)
	for _, re := range res {
		Size += int64(len(re))
	}
	return res, int64(len(res)), 0, Size
}

func newBlock() *Block {
	return &Block{
		Data: make([][]byte, 0, blockEntryNums),
		Nums: 0,
		Size: 0,
	}
}
