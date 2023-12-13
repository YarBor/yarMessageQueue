package Server

import (
	"BorsMQ"
	"encoding/json"
	"errors"
	"strings"
)

type Subscriber struct {
	Topic string
	Model string
}

const (
	AllowSubscriberTopicCharacter = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.>*"
	SubsModelPush                 = "PushModel"
	SubsModelPull                 = "PullModel"
	SubsModelUnknown              = "UnknownModel"
)

func (s *Subscriber) GetTopic() string { return s.Topic }
func SubscriberCheckTopicIllegal(t string) error {
	if t == "" || strings.IndexFunc(t, func(r rune) bool {
		return strings.IndexRune(AllowSubscriberTopicCharacter, r) == -1
	}) != -1 {
		return errors.New(BorsMQ.ErrorSubscriberTopicIllegal)
	}
	return nil
}

func (s *Subscriber) SetTopic(t string) error {
	if err := SubscriberCheckTopicIllegal(t); err != nil {
		return err
	}
	for tt := t; ; {
		i := strings.Index(tt, "*")
		if i == -1 {
			break
		}
		if i != 0 && tt[i-1] != '.' {
			return errors.New(BorsMQ.ErrorSubscriberTopicIllegal)
		}
		if i != len(tt)-1 && tt[i+1] != '.' {
			return errors.New(BorsMQ.ErrorSubscriberTopicIllegal)
		}
		tt = tt[i+1:]
	}
	for tt := t; ; {
		i := strings.Index(tt, ">")
		if i == -1 {
			break
		}
		if i != 0 && tt[i-1] != '.' {
			return errors.New(BorsMQ.ErrorSubscriberTopicIllegal)
		}
		if i != len(tt)-1 {
			return errors.New(BorsMQ.ErrorSubscriberTopicIllegal)
		}
		tt = tt[i+1:]
	}
	s.Topic = t
	return nil
}
func (s *Subscriber) SetSubsModel(m string) error {
	switch m {
	case SubsModelPull:
		s.Model = SubsModelPull
		break
	case SubsModelPush:
		s.Model = SubsModelPush
		break
	default:
		return errors.New(BorsMQ.ErrorUnknownSubsModel)
	}
	return nil
}
func (s *Subscriber) String() string {
	b, err := json.Marshal(*s)
	if err != nil {
		return ""
	} else {
		return string(b)
	}
}
func NewSubscriber() *Subscriber {
	return &Subscriber{
		Topic: "",
		Model: SubsModelUnknown,
	}
}
