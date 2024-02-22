package Random

import (
	"math/rand"
)

const letterBytes = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

//var autoincrementId int64
//
//func GetAutoincrementId() int64 {
//	return atomic.AddInt64(&autoincrementId, 1) - 1
//}
