package MqServer

const (
	ServerStatStop    = 0
	ServerStatWorking = 1
	ServerStatIniting = 2
)

type MqServerInfo struct {
	stat         int
	Name         string
	SelfUrl      string
	RaftUrl      string
	clustersInfo []MqServerInfo
}

type Option func(info *MqServerInfo)

func (m *MqServerInfo) getState() int { return m.stat }

func SetName(name string) Option {
	return func(info *MqServerInfo) { info.Name = name }
}

func SetServerURL(url string) Option {
	return func(info *MqServerInfo) { info.SelfUrl = url }
}

func AddClustersInfo(name, url string) Option {
	return func(info *MqServerInfo) {
		info.clustersInfo = append(info.clustersInfo, MqServerInfo{
			Name:    name,
			SelfUrl: url,
		})
	}
}
