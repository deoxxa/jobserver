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
	ErrTimeout  = errors.New("timed out")
	ErrNoJobs   = errors.New("no jobs")
	ErrNotFound = errors.New("not found")
)

type Job struct {
	ID        string
	Queue     string
	Priority  float64
	HoldUntil time.Time
	TTR       time.Duration
	Content   string
}

type Client struct {
	m       sync.RWMutex
	err     error
	socket  *net.UDPConn
	remote  net.Addr
	pending map[string]chan protocol.Message
	timeout time.Duration
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
		timeout: time.Second,
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

func (c *Client) req(m protocol.Message) (protocol.Message, error) {
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
	case <-time.After(c.timeout):
		return nil, ErrTimeout
	}
}

func (c *Client) SetTimeout(t time.Duration) {
	c.timeout = t
}

func (c *Client) Ping() (time.Duration, error) {
	before := time.Now()
	if _, err := c.req(&protocol.PingMessage{}); err != nil {
		return 0, err
	}

	return time.Now().Sub(before), nil
}

func (c *Client) Put(queue, id, content string, priority float64, holdUntil time.Time, ttr time.Duration) error {
	m := protocol.JobMessage{
		Queue:     queue,
		ID:        id,
		Priority:  priority,
		HoldUntil: holdUntil.Unix(),
		TTR:       uint64(ttr / time.Second),
		Content:   content,
	}

	r, err := c.req(&m)
	if err != nil {
		return err
	}

	switch r := r.(type) {
	case *protocol.SuccessMessage:
		return nil
	case *protocol.ErrorMessage:
		return errors.New(r.Reason)
	default:
		return ErrUnhandledType(fmt.Errorf("can't handle message type %T", r))
	}
}

func (c *Client) Reserve(queue string) (*Job, error) {
	r, err := c.req(&protocol.ReserveMessage{Queue: queue})
	if err != nil {
		return nil, err
	}

	switch r := r.(type) {
	case *protocol.JobMessage:
		return &Job{
			ID:        r.ID,
			Queue:     r.Queue,
			Priority:  r.Priority,
			HoldUntil: time.Unix(r.HoldUntil, 0),
			TTR:       time.Duration(r.TTR) * time.Second,
			Content:   r.Content,
		}, nil
	case *protocol.ErrorMessage:
		if r.Reason == "empty" {
			return nil, ErrNoJobs
		}
		return nil, errors.New(r.Reason)
	default:
		return nil, ErrUnhandledType(fmt.Errorf("can't handle message type %T", r))
	}
}

func (c *Client) ReserveWait(queue string) (*Job, error) {
	for {
		j, err := c.Reserve(queue)
		switch err {
		case nil:
			return j, nil
		case ErrNoJobs:
			time.Sleep(time.Second)
		default:
			return nil, err
		}
	}
}

func (c *Client) Peek(queue string) (*Job, error) {
	r, err := c.req(&protocol.PeekMessage{Queue: queue})
	if err != nil {
		return nil, err
	}

	switch r := r.(type) {
	case *protocol.JobMessage:
		return &Job{
			ID:        r.ID,
			Queue:     r.Queue,
			Priority:  r.Priority,
			HoldUntil: time.Unix(r.HoldUntil, 0),
			TTR:       time.Duration(r.TTR) * time.Second,
			Content:   r.Content,
		}, nil
	case *protocol.ErrorMessage:
		if r.Reason == "empty" {
			return nil, ErrNoJobs
		}
		return nil, errors.New(r.Reason)
	default:
		return nil, ErrUnhandledType(fmt.Errorf("can't handle message type %T", r))
	}
}

func (c *Client) Delete(queue, id string) error {
	r, err := c.req(&protocol.DeleteMessage{Queue: queue, ID: id})
	if err != nil {
		return err
	}

	switch r := r.(type) {
	case *protocol.SuccessMessage:
		return nil
	case *protocol.ErrorMessage:
		if r.Reason == "not found" {
			return ErrNotFound
		}
		return errors.New(r.Reason)
	default:
		return ErrUnhandledType(fmt.Errorf("can't handle message type %T", r))
	}
}
