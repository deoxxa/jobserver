package protocol

import (
	"fmt"
)

type PingMessage struct {
	Key string
}

func (m PingMessage) GetKey() string     { return m.Key }
func (m *PingMessage) SetKey(key string) { m.Key = key }
func (m PingMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("ping key=%s", m.Key))
}

type PutMessage struct {
	Key       string
	ID        string
	Queue     string
	Priority  float64
	HoldUntil int64
	TTR       uint64
	Content   string
}

func (m PutMessage) GetKey() string     { return m.Key }
func (m *PutMessage) SetKey(key string) { m.Key = key }
func (m PutMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("put key=%s id=%s queue=%s priority=%#v hold_until=%d ttr=%d content=%q", m.Key, m.ID, m.Queue, m.Priority, m.HoldUntil, m.TTR, m.Content))
}

type QueuedMessage struct {
	Key    string
	Action string
}

func (m QueuedMessage) GetKey() string     { return m.Key }
func (m *QueuedMessage) SetKey(key string) { m.Key = key }
func (m QueuedMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("queued key=%s action=%s", m.Key, m.Action))
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

type ReservedMessage struct {
	Key     string
	ID      string
	Content string
}

func (m ReservedMessage) GetKey() string     { return m.Key }
func (m *ReservedMessage) SetKey(key string) { m.Key = key }
func (m ReservedMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("reserved key=%s id=%s content=%q", m.Key, m.ID, m.Content))
}

type ReserveFailedMessage struct {
	Key    string
	Reason string
}

func (m ReserveFailedMessage) GetKey() string     { return m.Key }
func (m *ReserveFailedMessage) SetKey(key string) { m.Key = key }
func (m ReserveFailedMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("reserve_failed key=%s reason=%q", m.Key, m.Reason))
}

type DeleteMessage struct {
	Key string
	ID  string
}

func (m DeleteMessage) GetKey() string     { return m.Key }
func (m *DeleteMessage) SetKey(key string) { m.Key = key }
func (m DeleteMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("delete key=%s id=%s", m.Key, m.ID))
}

type DeletedMessage struct {
	Key     string
	Existed bool
}

func (m DeletedMessage) GetKey() string     { return m.Key }
func (m *DeletedMessage) SetKey(key string) { m.Key = key }
func (m DeletedMessage) Serialise() []byte {
	return []byte(fmt.Sprintf("deleted key=%s existed=%v", m.Key, m.Existed))
}
