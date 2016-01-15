package protocol // import "fknsrs.biz/p/jobserver/internal/protocol"

import (
	"bytes"
	"fmt"

	"github.com/kr/logfmt"
)

type (
	ErrUnknownType error
)

var DefaultParser = NewParser(map[string]func() Message{
	"delete":  func() Message { return &DeleteMessage{} },
	"error":   func() Message { return &ErrorMessage{} },
	"job":     func() Message { return &JobMessage{} },
	"peek":    func() Message { return &PeekMessage{} },
	"ping":    func() Message { return &PingMessage{} },
	"reserve": func() Message { return &ReserveMessage{} },
	"success": func() Message { return &SuccessMessage{} },
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
