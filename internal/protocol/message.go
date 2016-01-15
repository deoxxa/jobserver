package protocol // import "fknsrs.biz/p/jobserver/internal/protocol"

var (
	MessageSize = 1024 * 16
)

type Message interface {
	GetKey() string
	SetKey(key string)
	Serialise() []byte
}
