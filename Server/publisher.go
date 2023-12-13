package Server

import (
	"BorsMQ"
	"encoding/json"
	"errors"
	"strings"
)

const (
	AllowPublisherTopicCharacter = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789."
)

func PublisherCheckTopicIllegal(t string) error {
	if t == "" || strings.IndexFunc(t, func(r rune) bool {
		return strings.IndexRune(AllowPublisherTopicCharacter, r) == -1
	}) != -1 {
		return errors.New(BorsMQ.ErrorPublisherTopicIllegal)
	}
	return nil
}

type Publisher struct {
	Topic string
	route *RouteTreeNode
}

func (p *Publisher) SetRouteNode(route *RouteTreeNode) {
	p.route = route
}
func (p *Publisher) String() string {
	b, err := json.Marshal(*p)
	if err != nil {
		return ""
	} else {
		return string(b)
	}
}

func (p *Publisher) GetTopic() string {
	return p.Topic
}

func NewPublisher() *Publisher {
	return &Publisher{
		Topic: "",
	}
}

func (p *Publisher) SetTopic(topic string) error {
	if err := PublisherCheckTopicIllegal(topic); err != nil {
		return err
	}
	p.Topic = topic
	return nil
}
