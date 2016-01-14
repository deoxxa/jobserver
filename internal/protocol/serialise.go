package protocol

func Serialise(m Message) []byte {
	return m.Serialise()
}
