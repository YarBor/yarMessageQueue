package Server

import "testing"

func TestSubscriberCheckTopicIllegal(t *testing.T) {
	topic := "123.1213.123.123"
	err := SubscriberCheckTopicIllegal(topic)
	if err != nil {
		t.Fatal(err.Error())
	}
	topic = "123sgfd你好哦.qqwer1213.1qwer23as.1dsfa23"
	err = SubscriberCheckTopicIllegal(topic)
	if err == nil {
		t.Fatal(err.Error())
	}
	topic = "12 3.1213.123.123"
	err = SubscriberCheckTopicIllegal(topic)
	if err == nil {
		t.Fatal(err.Error())
	}
	topic = "123[]'/..1213.123.123"
	err = SubscriberCheckTopicIllegal(topic)
	if err == nil {
		t.Fatal(err.Error())
	}
	topic = "123.1213reaefasdf.123.123"
	err = SubscriberCheckTopicIllegal(topic)
	if err != nil {
		t.Fatal(err.Error())
	}
	topic = ""
	err = SubscriberCheckTopicIllegal(topic)
	if err == nil {
		t.Fatal(err.Error())
	}
}

func TestSubscriber_SetTopic(t *testing.T) {
	i := NewSubscriber()
	i.SetTopic("foo.i.>")
	t.Log(i.Topic)
	i.SetTopic("foo.i.>asdf")
	t.Log(i.Topic)
	i.SetTopic("foo.i.asdf>")
	t.Log(i.Topic)
	i.SetTopic("foo.i.asdf.*.>")
	t.Log(i.Topic)
	i.SetTopic(">")
	t.Log(i.Topic)
	i.SetTopic("")
	t.Log(i.Topic)
}
