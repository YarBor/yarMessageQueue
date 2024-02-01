package MqServer

import (
	"MqServer/RaftServer"
	"sync"
	"testing"
)

func TestMetaDataController_MakeSnapshot(t *testing.T) {
	type fields struct {
		MetaDataRaft *RaftServer.RaftNode
		mu           sync.RWMutex
		MD           *MetaData
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{name: "1", fields: fields{
			MD: NewMetaData(),
		}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdc := &MetaDataController{
				MetaDataRaft: tt.fields.MetaDataRaft,
				MD:           tt.fields.MD,
			}
			if got := mdc.MakeSnapshot(); got != nil {
				println(got)
			}
		})
	}
}
