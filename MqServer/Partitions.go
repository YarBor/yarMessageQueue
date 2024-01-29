package MqServer

type PartitionsController struct {
	P map[string]*partition
}

type partition struct {
	P             string
	T             string
	Entries       [][]byte
	ConsumeOffset int
	Consumers     []Consumer
}

func (p *partition) Handle(interface{}) error {

}

func (p *partition) MakeSnapshot() []byte {

}

func (p *partition) LoadSnapshot([]byte) {

}
