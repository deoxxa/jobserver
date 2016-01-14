package protocol

import (
	"bytes"
	"fmt"

	"github.com/kr/logfmt"
)

type (
	ErrUnknownType error
)

var DefaultParser = NewParser(map[string]func() Message{
	"ping":           func() Message { return &PingMessage{} },
	"put":            func() Message { return &PutMessage{} },
	"queued":         func() Message { return &QueuedMessage{} },
	"reserve":        func() Message { return &ReserveMessage{} },
	"reserved":       func() Message { return &ReservedMessage{} },
	"reserve_failed": func() Message { return &ReserveFailedMessage{} },
	"delete":         func() Message { return &DeleteMessage{} },
	"deleted":        func() Message { return &DeletedMessage{} },
})

func Parse(d []byte) (Message, error) {
	return DefaultParser.Parse(d)
}

type Parser struct {
	types map[string]func() Message
}

func NewParser(types map[string]func() Message) *Parser {
	return &Parser{types: types}
}

func (p *Parser) Types() []string {
	r := make([]string, len(p.types))

	i := 0
	for k := range p.types {
		r[i] = k
		i++
	}

	return r
}

func (p *Parser) Parse(d []byte) (Message, error) {
	bits := bytes.SplitN(d, []byte(" "), 2)
	name, rest := bits[0], []byte("")
	if len(bits) > 1 {
		rest = bits[1]
	}

	fn, ok := p.types[string(name)]
	if !ok {
		return nil, ErrUnknownType(fmt.Errorf("unknown type %q", name))
	}

	m := fn()

	return m, logfmt.Unmarshal(rest, m)
}
