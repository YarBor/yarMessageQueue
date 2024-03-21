package MqServer

import (
	"BorsMqServer/Random"
	"BorsMqServer/common"
	"errors"
	"fmt"
)

func mergeMaps(map1, map2 map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// 合并 map1 和 map2 中的键值对
	for key, value := range map1 {
		result[key] = value
	}

	for key, value := range map2 {
		// 如果 key 在 map1 中不存在，直接添加到结果中
		if _, ok := result[key]; !ok {
			result[key] = value
			continue
		}

		// 如果 value 是 map[string]interface{} 类型，递归合并
		if nestedMap1, ok := result[key].(map[string]interface{}); ok {
			if nestedMap2, ok := value.(map[string]interface{}); ok {
				result[key] = mergeMaps(nestedMap1, nestedMap2)
				continue
			}
		}

		// 否则，直接覆盖值
		result[key] = value
	}

	return result
}

type BrokerOptions struct {
	data    map[string]interface{}
	opts    []common.BuildOptions
	err     error
	IsBuild bool
}

func NewBrokerOptions() *BrokerOptions {
	return &BrokerOptions{
		opts:    []common.BuildOptions{},
		data:    make(map[string]interface{}),
		err:     errors.New("Options Need Build And Check"),
		IsBuild: false,
	}
}

func NewBrokerOptionsFormFields(data map[string]interface{}) *BrokerOptions {
	return &BrokerOptions{
		data:    data,
		opts:    []common.BuildOptions{},
		err:     errors.New("Options Need Build And Check"),
		IsBuild: false,
	}
}

func (o *BrokerOptions) With(options ...common.BuildOptions) *BrokerOptions {
	o.opts = append(o.opts, options...)
	return o
}

func (o *BrokerOptions) Build() (*BrokerOptions, error) {
	for _, option := range o.opts {
		service, input, err := option()
		if err != nil {
			o.err = err
			return nil, err
		}
		existingData, exists := o.data[service]
		if exists {
			// 合并数据
			if existingMap, ok := existingData.(map[string]interface{}); !ok {
				panic(o)
			} else {
				inputMap, ok := input.(map[string]interface{})
				if !ok {
					panic(o)
				}
				for key, value := range inputMap {
					existingMap[key] = value
				}
			}
		} else {
			o.data[service] = input
		}
	}
	if _, ok := o.data["BrokerKey"]; !ok {
		o.data["BrokerKey"] = Random.RandStringBytes(16)
	}
	if _, ok := o.data["BrokerToRegisterCenterHeartBeatSession"]; !ok {
		o.data["BrokerToRegisterCenterHeartBeatSession"] = common.BrokerToRegisterCenterHeartBeatSession
	}
	//o.data["IsMetaDataServer"] = true
	if i, ok := o.data["IsMetaDataServer"].(bool); ok && i {
		if _, ok := o.data["MetadataServerInfo"]; !ok {
			o.data["MetadataServerInfo"] = make(map[string]interface{})
		}
		o.data["MetadataServerInfo"].(map[string]interface{})[o.data["BrokerID"].(string)] = map[string]interface{}{
			"RaftUrl":          o.data["RaftServerAddr"].(map[string]interface{})["Url"].(string),
			"Url":              o.data["BrokerAddr"].(string),
			"HeartBeatSession": o.data["BrokerToRegisterCenterHeartBeatSession"].(int32),
		}
	}
	o.IsBuild = true
	return o.Check()
}

func (o *BrokerOptions) Check() (*BrokerOptions, error) {
	o.err = nil
	if addr, ok := o.data["RaftServerAddr"]; !ok {
		return nil, fmt.Errorf("Need Set RaftServerAddr")
	} else if addrs, ok := addr.(map[string]interface{}); !ok || len(addrs) > 1 {
		return nil, fmt.Errorf("RaftServerAddr , Need To Be Once")
	}
	if info, ok := o.data["MetadataServerInfo"]; !ok || len(info.(map[string]interface{})) == 0 {
		return nil, fmt.Errorf("need Set MetadataServerInfo")
	}
	if ID, ok := o.data["BrokerID"]; !ok {
		o.data["BrokerID"] = Random.RandStringBytes(16)
	} else if ID == "" {
		return nil, fmt.Errorf("BrokerID Is Empty")
	}
	if addr, ok := o.data["BrokerAddr"]; !ok || addr == "" {
		return nil, fmt.Errorf("BrokerAddr Is Empty")
	}
	if addr, ok := o.data["BrokerKey"]; !ok || addr == "" {
		return nil, fmt.Errorf("Key Is Empty")
	}
	return o, nil
}

func (o *BrokerOptions) Merge(o2 *BrokerOptions) (*BrokerOptions, error) {
	if o.IsBuild || o2.IsBuild {
		return nil, fmt.Errorf("Illegal options for merge , must be unBuild")
	}
	return &BrokerOptions{
		data: mergeMaps(o.data, o2.data),
		opts: append(append(make([]common.BuildOptions, 0), o.opts...), o2.opts...),
		err:  errors.New("Options Need Build And Check"),
	}, nil
}
