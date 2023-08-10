package consumer

import (
	protocal_message "c_k/protocal.message"
)

// const SerialVersionUID uint64 = 1234034768477580009

type Message struct {
	serialVersionUID uint64
	ID               int64
	Entries          []*protocal_message.Entry
	Raw              bool
	RawEntries       [][]byte
}

func NewMessage(id int64, entries []*protocal_message.Entry) *Message {
	return &Message{
		serialVersionUID: 1234034768477580009,
		ID:               id,
		Entries:          entries,
		Raw:              false,
	}
}

func NewRawMessage(id int64, entries [][]byte) *Message {
	return &Message{
		serialVersionUID: 1234034768477580009,
		ID:               id,
		Raw:              true,
		RawEntries:       entries,
	}
}

func (m *Message) AddEntry(entry *protocal_message.Entry) {
	m.Entries = append(m.Entries, entry)
}

func (m *Message) AddRawEntry(rawEntry []byte) {
	m.RawEntries = append(m.RawEntries, rawEntry)
}
