package Server

import (
	"testing"
)

func newSubs(s string) *Subscriber {
	i := NewSubscriber()
	i.SetTopic(s)
	return i
}
func newPub(s string) *Publisher {
	i := NewPublisher()
	i.SetTopic(s)
	return i
}
func TestNewSubscriber(t *testing.T) {
	err := routeRoot.RegisterSubscriber(newSubs("foo"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.a.a.a.a"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.a.a.a"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.b"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.*"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.*.*"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.*.a"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.>"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.*.>"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.a.a"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.a.b"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
	err = routeRoot.RegisterSubscriber(newSubs("foo.a.a.*"))
	if err != nil {
		t.Log(err)
	}
	t.Log("\n" + routeRoot.String())
}
func registerSuberAndLog(s string, b *testing.T) *Subscriber {
	i := newSubs(s)
	routeRoot.RegisterSubscriber(i)
	b.Log(routeRoot.String())
	return i
}
func registerPuberAndLog(s string, b *testing.T) {
	i := newPub(s)
	routeRoot.RegisterPublisher(i)
	b.Log(routeRoot.String())
}
func TestRouteTreeNode_RegisterPublisher(b *testing.T) {
	registerSuberAndLog(">", b)
	registerPuberAndLog("1.1.1.1", b)
	registerSuberAndLog("2.>", b)
	registerPuberAndLog("2.1", b)
	registerSuberAndLog("234.234.>", b)
	registerPuberAndLog("234.1.1.1", b)
	registerSuberAndLog("234.*.>", b)
	registerPuberAndLog("2.1.1", b)
	registerPuberAndLog("1.1.1", b)
	registerSuberAndLog("1.>", b)
}
