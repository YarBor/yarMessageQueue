package message_memory

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	Log "github.com/YarBor/BorsMqServer/logger"
	"github.com/YarBor/BorsMqServer/random"
)

func TestNewMessageEntry(t *testing.T) {
	got := NewMessageEntry(10, 1e5, 3)
	fmt.Printf("%#v", got)
}

func TestMessageEntry_Write_Normal(t *testing.T) {
	got := NewMessageEntry(100, 1e6, 3e3)
	fmt.Printf("%#v", got)
	for i := 0; i < 10; i++ {
		got.Write([]byte(random.RandStringBytes(1e3)))
	}
	assert.Equal(t, 9, int(got.EntriesStorageNow))
	assert.Equal(t, 9000, int(got.SizeStorageNow))
}

func TestMessageEntry_Write_Lose_Entry(t *testing.T) {
	got := NewMessageEntry(100, 1e6, 5e3)
	fmt.Printf("%#v", got)
	for i := 0; i < 105; i++ {
		got.Write([]byte(random.RandStringBytes(1e3)))
	}
	assert.Equal(t, 100, int(got.EntriesStorageNow))
	assert.Equal(t, int(100*1e3), int(got.SizeStorageNow))
}

func TestMessageEntry_Write_Lose_Size(t *testing.T) {
	got := NewMessageEntry(100, 5e3, 3e3)
	fmt.Printf("%#v", got)
	for i := 0; i < 16; i++ {
		got.Write([]byte(random.RandStringBytes(1e3)))
	}
	// may clear time
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 4, int(got.EntriesStorageNow))
	assert.Equal(t, 4000, int(got.SizeStorageNow))
}

func TestMessageEntry_Write_Read(t *testing.T) {
	got := NewMessageEntry(100, 1e6, 3e3)
	fmt.Printf("%#v", got)
	checknum := []int{}
	checkstr := []string{}
	for i := 0; i < 10; i++ {
		str := random.RandStringBytes(1e3)
		if 0 == int(rand.Int31()%3) {
			checknum = append(checknum, i)
			checkstr = append(checkstr, str)
		}
		got.Write([]byte(str))
	}

	for i, str := range checkstr {
		BeginOff, Data, ReadNum := got.Read(int64(checknum[i]), 1, 1e4)
		if checknum[i] != int(BeginOff) {
			panic(BeginOff)
		}
		if 1 != int(ReadNum) {
			panic(ReadNum)
		}
		if str != string(Data[0]) {
			panic(Data[0])
		}
	}

}

func TestMessageEntry_Write_Read_Lose(t *testing.T) {
	Log.SetLogLevel(Log.LogLevel_TRACE)
	got := NewMessageEntry(100, 1e6, 3e3)
	//mu := sync.Mutex{}
	check_map := map[int64]string{}
	t.Logf("%#v", got)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i <= 1e9; i++ {
			select {
			case _ = <-ctx.Done():
				return
			default:
				str := strconv.Itoa(i) + "\n" + random.RandStringBytes(1)
				//mu.Lock()
				//check_map[int64(i)] = str
				//mu.Unlock()
				got.Write([]byte(str))
				got.Write([]byte(str))
				got.Write([]byte(str))
				//time.Sleep(7 * time.Millisecond)
			}
		}
	}()
	go func() {
		defer wg.Done()
		off := int64(0)
		for {
			select {
			case _ = <-ctx.Done():
				goto done
			default:
				offbegin, _, num := got.Read(off, 10, 1e5)
				off = offbegin + num
				//mu.Lock()
				//for i, datum := range data {
				//	if str, ok := check_map[offbegin+int64(i)]; !ok || str != string(datum) {
				//		t.Errorf("%v is not %v", check_map[offbegin+int64(i)], datum)
				//		panic(1)
				//	} else {
				//		Log.DEBUG(string(datum[:15]), "\n")
				//		//delete(check_map, offbegin+int64(i))
				//	}
				//}
				////mu.Unlock()
				println(off)
				//time.Sleep(10 * time.Millisecond)
			}
		}
	done:
	}()

	wg.Wait()
	assert.Less(t, len(check_map), 10)
}

func TestMessageEntry_MakeSnapshot_LoadSnapShot(t *testing.T) {
	got := NewMessageEntry(1030, 1e6, 3e3)
	check := NewMessageEntry(1000, 10000, 010000)
	fmt.Printf("%#v", got)
	for i := 0; i < 190; i++ {
		got.Write([]byte(random.RandStringBytes(1e3)))
	}
	data := got.MakeSnapshot()
	check.LoadSnapshot(data)
	assert.Equal(t, got.MaxEntries, check.MaxEntries)
	assert.Equal(t, got.MaxSize, check.MaxSize)
	assert.Equal(t, got.EntriesStorageNow, check.EntriesStorageNow)
	assert.Equal(t, got.SizeStorageNow, check.SizeStorageNow)
	assert.Equal(t, got.En.BeginOffset, check.En.BeginOffset)
	assert.Equal(t, got.En.EndOffset, check.En.EndOffset)
	assert.Equal(t, got.En.EntryMaxSizeOf_1Block, check.En.EntryMaxSizeOf_1Block)
	assert.Equal(t, len(got.En.Ens), len(check.En.Ens))
}
