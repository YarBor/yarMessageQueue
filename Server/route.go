package Server

import (
	"BorsMQ"
	"encoding/json"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
)

type AssSubscriber struct {
	SubTopics  []string
	Subscriber *Subscriber
}

type RouteTreeNode struct {
	mu         sync.RWMutex
	father     *RouteTreeNode
	RoutesNode map[string]*RouteTreeNode
	Topic      string
	AssCache   []*AssSubscriber
	Gss        []*Subscriber
	// Subscribers registered this topic
	Subscribers []*Subscriber
	// Publishers registered this topic
	Publishers []*Publisher
	CheckNum   int64
}

var routeRoot *RouteTreeNode

func GetRouteRoot() *RouteTreeNode {
	return routeRoot
}

func newRouteTreeNode(name string) *RouteTreeNode {
	return &RouteTreeNode{
		Topic:       name,
		father:      nil,
		RoutesNode:  make(map[string]*RouteTreeNode),
		AssCache:    nil,
		Gss:         nil,
		Subscribers: make([]*Subscriber, 0),
		Publishers:  make([]*Publisher, 0),
		CheckNum:    rand.Int63(),
	}
}

// Lock should be held on entry
func (r *RouteTreeNode) getNextRoute(subTopic string) *RouteTreeNode {
	res, ok := r.RoutesNode[subTopic]
	if ok {
		return res
	}
	return nil
}

// Lock should be held on entry
func (r *RouteTreeNode) matchAssSubscribers(Topic string) []*Subscriber {
	if r.AssCache == nil {
		return nil
	}
	var subTopics []string
	rtps, stps := strings.Split(r.Topic, "."), strings.Split(Topic, ".")
	// check match
	for index := 0; index < len(rtps); index++ {
		if rtps[index] != stps[index] {
			return nil
		}
	}
	subTopics = stps[len(rtps):]
	var res []*Subscriber
	for _, i := range r.AssCache {
		if len(i.SubTopics) == len(subTopics) || (len(i.SubTopics) < len(subTopics) && i.SubTopics[len(i.SubTopics)-1] == ">") {
			index := 0
			for ; index < len(i.SubTopics); index++ {
				switch i.SubTopics[index] {
				case "*":
					continue
				case ">":
					if res == nil {
						res = make([]*Subscriber, 0)
					}
					res = append(res, i.Subscriber)
					goto next
				case subTopics[index]:
					continue
				default:
					goto next
				}
			}
			if index == len(subTopics) {
				if res == nil {
					res = make([]*Subscriber, 0)
				}
				res = append(res, i.Subscriber)
			}
		}
	next:
	}
	return res
}

// Lock should be held on entry
func (r *RouteTreeNode) setNextRoute(subTopic string) *RouteTreeNode {
	newRouteNode := newRouteTreeNode(r.Topic + "." + subTopic)
	newRouteNode.father = r
	r.RoutesNode[subTopic] = newRouteNode
	// 在创建的时候 所有祖先都是上锁的状态
	for i := newRouteNode.father; i != nil; i = i.father {
		if arr := i.matchAssSubscribers(newRouteNode.Topic); arr != nil {
			newRouteNode.Subscribers = append(newRouteNode.Subscribers, arr...)
		}
		if i.Gss != nil {
			newRouteNode.Subscribers = append(newRouteNode.Subscribers, i.Gss...)
		}
	}
	return newRouteNode
}

// Lock should be held on entry
func (r *RouteTreeNode) deleteSubNode(subTopic string) {
	delete(r.RoutesNode, subTopic)
}

//	func (r *RouteTreeNode) RegisterSubscriber(s *Subscriber) error {
//		if err := SubscriberCheckTopicIllegal(s.Topic); err != nil {
//			return err
//		}
//		ln := r
//		ts := strings.Split(s.Topic, ".")
//		for ; len(ts) > 0; ts = ts[1:] {
//			if ts[0] == ">" {
//				ln.mu.Lock()
//				if ln.gss == nil {
//					ln.gss = make([]*Subscriber, 0)
//				}
//				ln.gss = append(ln.gss, s)
//				ln.mu.Unlock()
//				return nil
//			} else if ts[0] == "*" {
//				ln.mu.Lock()
//				if ln.assCache == nil {
//					ln.assCache = make([]*AssSubscriber, 0)
//				}
//				ln.assCache = append(ln.assCache, &AssSubscriber{
//					subTopics:  ts,
//					subscriber: s,
//				})
//				ln.mu.Unlock()
//				return nil
//			} else {
//				var nextNode *RouteTreeNode
//				if nextNode := ln.getNextRoute(ts[0]); nextNode == nil {
//					nextNode = ln.setNextRoute(ts[0])
//				}
//				ln = nextNode
//			}
//		}
//		ln.mu.Lock()
//		ln.Subscribers = append(ln.Subscribers, s)
//		ln.mu.Unlock()
//		return nil
//	}
func (ln *RouteTreeNode) doDirty() {
	atomic.AddInt64(&ln.CheckNum, 1)
}
func (ln *RouteTreeNode) registerSubscriber(tps []string, s *Subscriber, isMounted bool) error {
	ln.mu.RLock()
	if len(tps) == 0 {
		ln.mu.RUnlock()
		ln.mu.Lock()
		ln.Subscribers = append(ln.Subscribers, s)
		ln.doDirty()
		ln.mu.Unlock()
		return nil
	}
	if tps[0] == "*" {
		if !isMounted {
			isMounted = true
			ln.mu.RUnlock()
			ln.mu.Lock()
			if ln.AssCache == nil {
				ln.AssCache = make([]*AssSubscriber, 0)
			}
			ln.AssCache = append(ln.AssCache, &AssSubscriber{
				SubTopics:  tps[:],
				Subscriber: s,
			})
			ln.mu.Unlock()
			ln.mu.RLock()
		}
		for _, t := range ln.RoutesNode {
			t.mu.RLock()
			for _, q := range t.RoutesNode {
				if err := q.registerSubscriber(tps[1:], s, isMounted); err != nil {
					t.mu.RUnlock()
					ln.mu.RUnlock()
					return err
				}
			}
			t.mu.RLock()
		}
		ln.mu.RUnlock()
		return nil
	} else if tps[0] == ">" {
		ln.mu.RUnlock()
		ln.mu.Lock()
		if !isMounted {
			isMounted = true
			if ln.Gss == nil {
				ln.Gss = make([]*Subscriber, 0)
			}
			ln.Gss = append(ln.Gss, s)
		} else {
			ln.Subscribers = append(ln.Subscribers, s)
			ln.doDirty()
		}
		ln.mu.Unlock()
		ln.mu.RLock()
		for _, t := range ln.RoutesNode {
			if err := t.registerSubscriber(tps, s, isMounted); err != nil {
				ln.mu.RUnlock()
				return err
			}
		}
		ln.mu.RUnlock()
		return nil
	}
	i := ln.getNextRoute(tps[0])
	if i == nil {
		ln.mu.RUnlock()
		ln.mu.Lock()
		// ReCheck to avoid race
		if i = ln.getNextRoute(tps[0]); i == nil {
			i = ln.setNextRoute(tps[0])
		}
		ln.mu.Unlock()
		ln.mu.RLock()
	}
	err := i.registerSubscriber(tps[1:], s, false)
	ln.mu.RUnlock()
	return err
}
func (r *RouteTreeNode) RegisterSubscriber(s *Subscriber) error {
	if s == nil {
		return errors.New(BorsMQ.ErrorUB)
	} else if err := SubscriberCheckTopicIllegal(s.GetTopic()); err != nil {
		return err
	}
	return r.registerSubscriber(strings.Split(s.GetTopic(), "."), s, false)
}
func (r *RouteTreeNode) assSubscriberRemove(s *Subscriber, tps []string) error {
	if r.AssCache == nil {
		return errors.New(BorsMQ.ErrorUB)
	}
	r.mu.Lock()
	if tps == nil {
		for i, assSb := range r.AssCache {
			if s == assSb.Subscriber {
				tps = assSb.SubTopics
				r.AssCache = append(r.AssCache[:i], r.AssCache[i+1:]...)
				break
			}
		}
	}
	r.mu.Unlock()

	r.mu.RLock()
	var needDelete []string
	if tps[0] == ">" {
		for k, v := range r.RoutesNode {
			if err := v.gssSubscriberRemove(s, false); err != nil {
				r.mu.RUnlock()
				return err
			}
			if v.CheckClear() {
				if needDelete == nil {
					needDelete = make([]string, 0)
				}
				needDelete = append(needDelete, k)
			}
		}
	} else {
		for k, v := range r.RoutesNode {
			if k == tps[0] || tps[0] == "*" {
				if err := v.assSubscriberRemove(s, tps[1:]); err != nil {
					r.mu.RUnlock()
					return err
				}
				if v.CheckClear() {
					if needDelete == nil {
						needDelete = make([]string, 0)
					}
					needDelete = append(needDelete, k)
				}
			}
		}
	}
	r.mu.RUnlock()
	if needDelete != nil {
		r.mu.Lock()
		for _, v := range needDelete {
			if i, ok := r.RoutesNode[v]; ok && i.mu.TryLock() && i.CheckClear() {
				r.deleteSubNode(v)
				i.mu.Unlock()
			}
		}
		r.mu.Unlock()
	}
	return nil
}
func (r *RouteTreeNode) gssSubscriberRemove(s *Subscriber, needUnmount bool) error {
	if r.Gss == nil {
		return errors.New(BorsMQ.ErrorUB)
	}
	r.mu.Lock()
	if needUnmount {
		for i := range r.Gss {
			if r.Gss[i] == s {
				r.Gss = append(r.Gss[:i], r.Gss[i+1:]...)
				break
			}
		}
	}
	if needUnmount {
		needUnmount = false
	} else {
		for i := range r.Subscribers {
			if r.Subscribers[i] == s {
				r.Subscribers = append(r.Subscribers[:i], r.Subscribers[i+1:]...)
				r.doDirty()
				break
			}
		}
	}
	r.mu.Unlock()
	var needRemove []string
	r.mu.RLock()
	for k, v := range r.RoutesNode {
		if err := v.gssSubscriberRemove(s, needUnmount); err != nil {
			r.mu.RUnlock()
			return err
		}
		if v.CheckClear() {
			if needRemove == nil {
				needRemove = make([]string, 0)
			}
			needRemove = append(needRemove, k)
		}
	}
	r.mu.RUnlock()
	if needRemove != nil {
		r.mu.Lock()
		for _, v := range needRemove {
			if i, ok := r.RoutesNode[v]; ok && i.mu.TryLock() && i.CheckClear() {
				r.deleteSubNode(v)
				i.mu.Unlock()
			}
		}
		r.mu.Unlock()
	}
	return nil
}
func (r *RouteTreeNode) CheckClear() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return (r.Gss == nil || len(r.Gss) == 0) &&
		(r.AssCache == nil || len(r.AssCache) == 0) &&
		(len(r.RoutesNode) == 0) &&
		(len(r.Subscribers) == 0) &&
		(len(r.Publishers) == 0)
}
func (r *RouteTreeNode) UnRegisterSubscriber(s *Subscriber) error {
	stpc := s.GetTopic()
	if err := SubscriberCheckTopicIllegal(stpc); err != nil {
		return err
	}
	ln := r
	var err error
	for ts := strings.Split(stpc, "."); len(ts) > 0; ts = ts[1:] {
		if ts[0] == ">" {
			return ln.gssSubscriberRemove(s, true)
		} else if ts[0] == "*" {
			return ln.assSubscriberRemove(s, nil)
		} else {
			ln.mu.RLock()
			var nextNode *RouteTreeNode
			if nextNode = ln.getNextRoute(ts[0]); nextNode == nil {
				err = errors.New(BorsMQ.ErrorUB)
				break
			}
			ln = nextNode
		}
	}
	if err != nil {
		ln.mu.Lock()
		for i := range ln.Subscribers {
			if ln.Subscribers[i] == s {
				ln.Subscribers = append(ln.Subscribers[:i], ln.Subscribers[i+1:]...)
				ln.doDirty()
				break
			}
		}
		ln.mu.Unlock()
	} else {
		for ln != nil {
			f := ln.father
			ln.mu.RUnlock()
			ln = f
		}
		return err
	}
	for ln != nil {
		isNeedRemove := ln.CheckClear()
		subName := ln.Topic[strings.LastIndex(ln.Topic, ".")+1:]
		ln.mu.RUnlock()
		ln = ln.father
		if isNeedRemove && ln != routeRoot {
			ln.mu.RUnlock()
			ln.mu.Lock()
			if i, ok := ln.RoutesNode[subName]; ok && i.mu.TryLock() && i.CheckClear() {
				ln.deleteSubNode(subName)
				i.mu.Unlock()
			}
			ln.mu.Unlock()
			ln.mu.RLock()
		}
	}
	return nil
}
func init() {
	routeRoot = newRouteTreeNode("")
}

func (r *RouteTreeNode) String() string {
	b, e := json.Marshal(r)
	if e != nil {
		return "nil"
	} else {
		return string(b)
	}
}

func (r *RouteTreeNode) registerPublisher(tps []string, p *Publisher) error {
	ln := r
	for _, s := range tps {
		ln.mu.RLock()
		i := ln.getNextRoute(s)
		if i == nil {
			ln.mu.RUnlock()
			ln.mu.Lock()
			i = ln.setNextRoute(s)
			ln.mu.Unlock()
			ln.mu.RLock()
		}
		ln = i
	}
	ln.mu.Lock()
	ln.Publishers = append(ln.Publishers, p)
	p.SetRouteNode(ln)
	f := ln.father
	ln.mu.Unlock()
	ln = f
	for ln != nil {
		f = ln.father
		ln.mu.RUnlock()
		ln = f
	}
	return nil
}
func (r *RouteTreeNode) RegisterPublisher(p *Publisher) error {
	if err := PublisherCheckTopicIllegal(p.GetTopic()); err != nil {
		return err
	}
	return r.registerPublisher(strings.Split(p.GetTopic(), "."), p)
}

func (r *RouteTreeNode) unregisterPublisher(tps []string, p *Publisher) (error, bool) {
	if tps == nil {
		panic(BorsMQ.ErrorUB)
	}
	if len(tps) == 0 {
		r.mu.Lock()
		defer r.mu.Unlock()
		for i := range r.Publishers {
			if r.Publishers[i] == p {
				r.Publishers = append(r.Publishers[:i], r.Publishers[i+1:]...)
				break
			}
		}
		return nil, r.CheckClear()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	j := r.getNextRoute(tps[0])
	if j == nil {
		panic(BorsMQ.ErrorUB)
	}
	err, isNeedDelete := j.unregisterPublisher(tps[1:], p)
	if isNeedDelete && j.mu.TryRLock() && j.CheckClear() {
		r.deleteSubNode(tps[0])
		j.mu.Unlock()
	}
	return err, r.CheckClear()
}
func (r *RouteTreeNode) UnRegisterPublisher(p *Publisher) error {
	if err := PublisherCheckTopicIllegal(p.GetTopic()); err != nil {
		return err
	}
	err, _ := r.unregisterPublisher(strings.Split(p.GetTopic(), "."), p)
	return err
}
