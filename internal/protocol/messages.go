package protocol // import "fknsrs.biz/p/jobserver/internal/protocol"

import (
	"fmt"
)

type DeleteMessage struct {
	Key   string
	Queue string
	ID    string
}

func (m DeleteMessage) GetKey() string     { return m.Key }
func (m *DeleteMessage) SetKey(key string) { m.Key = key }
func (m DeleteMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("delete key=%s queue=%s id=%s", m.Key, m.Queue, m.ID))
}

type ErrorMessage struct {
	Key    string
	Reason string
}

func (m ErrorMessage) GetKey() string     { return m.Key }
func (m *ErrorMessage) SetKey(key string) { m.Key = key }
func (m ErrorMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("error key=%s reason=%q", m.Key, m.Reason))
}

type JobMessage struct {
	Key       string
	ID        string
	Queue     string
	Priority  float64
	HoldUntil int64
	TTR       uint64
	Content   string
}

func (m JobMessage) GetKey() string     { return m.Key }
func (m *JobMessage) SetKey(key string) { m.Key = key }
func (m JobMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("job key=%s id=%s queue=%s priority=%#v hold_until=%d ttr=%d content=%q", m.Key, m.ID, m.Queue, m.Priority, m.HoldUntil, m.TTR, m.Content))
}

type PeekMessage struct {
	Key   string
	Queue string
}

func (m PeekMessage) GetKey() string     { return m.Key }
func (m *PeekMessage) SetKey(key string) { m.Key = key }
func (m PeekMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("peek key=%s queue=%s", m.Key, m.Queue))
}

type PingMessage struct {
	Key string
}

func (m PingMessage) GetKey() string     { return m.Key }
func (m *PingMessage) SetKey(key string) { m.Key = key }
func (m PingMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("ping key=%s", m.Key))
}

type ReserveMessage struct {
	Key   string
	Queue string
}

func (m ReserveMessage) GetKey() string     { return m.Key }
func (m *ReserveMessage) SetKey(key string) { m.Key = key }
func (m ReserveMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("reserve key=%s queue=%s", m.Key, m.Queue))
}

type SuccessMessage struct {
	Key string
}

func (m SuccessMessage) GetKey() string     { return m.Key }
func (m *SuccessMessage) SetKey(key string) { m.Key = key }
func (m SuccessMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("success key=%s", m.Key))
}
