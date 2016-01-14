package jobserver

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"fknsrs.biz/p/jobserver/internal/protocol"
)

type (
	ErrUnhandledType error
)

var (
	ErrTimeout = fmt.Errorf("timed out")
	ErrNoJobs  = fmt.Errorf("no jobs")
)

type Client struct {
	m       sync.RWMutex
	err     error
	socket  *net.UDPConn
	remote  net.Addr
	pending map[string]chan protocol.Message
}

func Dial(addr string) (*Client, error) {
	raddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return nil, err
	}

	s, err := net.DialUDP("udp4", nil, raddr)
	if err != nil {
		return nil, err
	}

	c := Client{
		socket:  s,
		remote:  raddr,
		pending: make(map[string]chan protocol.Message),
	}

	go c.run()

	return &c, nil
}

func (c *Client) run() {
	for {
		d := make([]byte, protocol.MessageSize)
		if _, _, err := c.socket.ReadFrom(d); err != nil {
			c.err = err
			return
		}

		m, err := protocol.Parse(d)
		if err != nil {
			continue
		}

		c.m.Lock()
		ch, ok := c.pending[m.GetKey()]
		if ok {
			delete(c.pending, m.GetKey())
			ch <- m
		}
		c.m.Unlock()
	}
}

func (c *Client) req(m protocol.Message, timeout time.Duration) (protocol.Message, error) {
	if c.err != nil {
		return nil, c.err
	}

	if m.GetKey() == "" {
		d := make([]byte, 8)
		if _, err := io.ReadFull(rand.Reader, d); err != nil {
			return nil, err
		}
		m.SetKey(hex.EncodeToString(d))
	}

	ch := make(chan protocol.Message, 1)

	c.m.Lock()
	c.pending[m.GetKey()] = ch
	c.m.Unlock()

	if _, err := c.socket.Write(protocol.Serialise(m)); err != nil {
		return nil, err
	}

	select {
	case r := <-ch:
		return r, nil
	case <-time.After(timeout):
		return nil, ErrTimeout
	}
}

func (c *Client) Ping() (time.Duration, error) {
	before := time.Now()
	if _, err := c.req(&protocol.PingMessage{}, time.Second); err != nil {
		return 0, err
	}

	return time.Now().Sub(before), nil
}

func (c *Client) Put(queue, id, content string, priority float64, holdUntil time.Time, ttr time.Duration) (string, error) {
	m := protocol.PutMessage{
		Queue:     queue,
		ID:        id,
		Priority:  priority,
		HoldUntil: holdUntil.Unix(),
		TTR:       uint64(ttr / time.Second),
		Content:   content,
	}

	r, err := c.req(&m, time.Second)
	if err != nil {
		return "", err
	}

	switch r := r.(type) {
	case *protocol.QueuedMessage:
		return r.Action, nil
	default:
		return "", ErrUnhandledType(fmt.Errorf("can't handle message type %T", r))
	}
}

func (c *Client) Reserve(queue string) (string, string, error) {
	r, err := c.req(&protocol.ReserveMessage{Queue: queue}, time.Second)
	if err != nil {
		return "", "", err
	}

	switch r := r.(type) {
	case *protocol.ReservedMessage:
		return r.ID, r.Content, nil
	case *protocol.ReserveFailedMessage:
		if r.Reason == "empty" {
			return "", "", ErrNoJobs
		}
		return "", "", errors.New(r.Reason)
	default:
		return "", "", ErrUnhandledType(fmt.Errorf("can't handle message type %T", r))
	}
}

func (c *Client) ReserveWait(queue string) (string, string, error) {
	for {
		id, content, err := c.Reserve(queue)
		switch err {
		case nil:
			return id, content, nil
		case ErrNoJobs:
			time.Sleep(time.Second)
		default:
			return "", "", err
		}
	}
}

func (c *Client) Delete(id string) (bool, error) {
	r, err := c.req(&protocol.DeleteMessage{ID: id}, time.Second)
	if err != nil {
		return false, err
	}

	switch r := r.(type) {
	case *protocol.DeletedMessage:
		return r.Existed, nil
	default:
		return false, ErrUnhandledType(fmt.Errorf("can't handle message type %T", r))
	}
}
