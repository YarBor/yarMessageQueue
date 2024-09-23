package message_memory

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"sync"
	"syscall"
)

import (
	"github.com/YarBor/BorsMqServer/common"
	Pack "github.com/YarBor/BorsMqServer/raft_server/pack"
	"github.com/YarBor/BorsMqServer/util"
)

type Block struct {
	mu        sync.RWMutex
	Path      string
	Index     []byte
	Data      []byte
	Nums      int64
	Size      int64
	file      *os.File
	IndexSize uint32

	Raw []byte
}

func (b *Block) MakeSnapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()
	bt := bytes.NewBuffer(nil)
	encode := Pack.NewEncoder(bt)
	err := encode.Encode(b.Path)
	if err != nil {
		panic(err)
	}
	err = encode.Encode(b.Raw)
	if err != nil {
		panic(err)
	}
	return bt.Bytes()
}

func SnapshotToBlock(bts []byte) *Block {
	var path string
	var raw []byte
	bf := bytes.NewBuffer(bts)
	decode := Pack.NewDecoder(bf)
	err := decode.Decode(&path)
	if err != nil {
		panic(err)
	}
	err = decode.Decode(&raw)
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		log.Printf("Sync snapshot false")
		return nil
	}
	num, err := f.Write(raw)
	if err != nil || num != len(raw) {
		log.Printf("Sync snapshot false")
		return nil
	}
	_ = f.Close()
	res, err := newBlockWithPath(path)
	if err != nil {
		log.Printf("Sync snapshot false")
		return nil
	}
	return res
}

// Safe
func (ebs *EntryBlocks) LoseEarliestBlock() *Block {
	ebs.mu.Lock()
	defer ebs.mu.Unlock()
	if len(ebs.Ens) <= 1 {
		return nil
	} else {
		last := ebs.Ens[0]
		_ = last.file.Close()
		_ = os.Remove(last.Path)
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
	i := b.IndexSize
	b.IndexSize++
	begin := 0
	if i != 0 {
		begin = int(binary.LittleEndian.Uint32(b.Index[i*4:]))
	}
	binary.LittleEndian.PutUint32(b.Index[i*4+4:], uint32(begin+len(entry)))
	copy(b.Data[begin:begin+len(entry)], entry)
}

func (b *Block) read(needReadEntBegin, entNum int64) (
	data [][]byte /*data*/, read_num int64 /*readNum*/, jump_num int64 /*jumpNum*/, read_size int64 /*ReadSize*/) {
	var res [][]byte
	if needReadEntBegin >= b.Nums {
		return nil, 0, b.Nums, 0
	} else {
		if needReadEntBegin+entNum >= b.Nums {
			for i := needReadEntBegin; i < b.Nums; i++ {
				res = append(res, b.readWithIndex(uint32(i)))
			}
		} else {
			for i := needReadEntBegin; i < needReadEntBegin+entNum; i++ {
				res = append(res, b.readWithIndex(uint32(i)))
			}
		}
	}
	Size := int64(0)
	for _, re := range res {
		Size += int64(len(re))
	}
	return res, int64(len(res)), 0, Size
}

func (b *Block) readWithIndex(index uint32) []byte {
	if index > b.IndexSize {
		return nil
	}
	_index := binary.LittleEndian.Uint32(b.Index[index*4:])
	var index_ uint32
	if index == b.IndexSize-1 {
		s, _ := b.file.Stat()
		index_ = uint32(s.Size())
	} else {
		index_ = binary.LittleEndian.Uint32(b.Index[(index+1)*4:])
	}
	res := make([]byte, index_-_index)
	copy(res, b.Data[index_:_index])
	return res
}

func newBlock(t, p string) (*Block, error) {
	path := common.DefaultLogFilePrefix + "/" + t + "/" + p
	err := util.CreatePath(path)
	if err != nil {
		return nil, err
	}
	return newBlockWithPath(path)
}

// newBlock 初始化一个 Block，并使用 mmap 映射文件到内存
func newBlockWithPath(path string) (*Block, error) {
	// 初始化 Block 结构体
	block := &Block{
		Path:      path,
		Index:     nil,
		Data:      nil,
		Nums:      0,
		Size:      0,
		file:      nil,
		IndexSize: uint32(common.DefaultMaxEntriesOf1Read),
	}

	// 打开文件
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	block.file = file

	// 获取文件大小
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()

	block.Size = size

	// mmap 映射文件到内存
	data, err := syscall.Mmap(int(file.Fd()), 0, int(max(size, int64(common.DefaultMaxSizeOf1Read))), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	block.Raw = data

	binary.LittleEndian.PutUint32(data[:], uint32(len(path)))
	copy(data[4:], path)
	binary.LittleEndian.PutUint32(data[4+len(path):], uint32(block.IndexSize))

	block.Index = data[8+len(path) : uint32(4+4+len(path))+block.IndexSize*4]
	block.Data = data[uint32(4+4+len(path))+block.IndexSize*4:]

	return block, nil
}
