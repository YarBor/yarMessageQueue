package MqServer

type BrokerOption func(*broker) error

//func WithMetaDataServer() BrokerOption {
//	return func(b *broker) error {
//
//	}
//}
//
//func WithConfigFile(path string) BrokerOption {
//	path = strings.TrimSpace(path)
//	index := strings.IndexAny(path, ".")
//	return func(bk *broker) error {
//		f, err := os.Open(path)
//		defer f.Close()
//		if err != nil {
//			return err
//		}
//		data, err1 := io.ReadAll(f)
//		if err1 != nil {
//			return err1
//		}
//		switch path[:index] {
//		case "yml", "yaml":
//			var outData interface{}
//			err = yaml.Unmarshal(data, outData)
//			if err != nil {
//				return err
//			}
//		case "json":
//			//case "xml":
//		}
//	}
//}
