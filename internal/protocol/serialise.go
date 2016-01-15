package protocol // import "fknsrs.biz/p/jobserver/internal/protocol"

func Serialise(m Message) []byte {
	return m.Serialise()
}
