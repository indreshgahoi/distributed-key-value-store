package main

import (
	"encoding/json"
	"fmt"
)

// Op is a state machine operation.
type Op string

const (
	OpPut    Op = "PUT"
	OpDelete Op = "DELETE"
)

// Command is the payload of one replicated log entry.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func (c Command) Encode() []byte {
	b, _ := json.Marshal(c) // a struct of strings always marshals
	return b
}

func DecodeCommand(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("malformed command: %w", err)
	}
	if c.Op != OpPut && c.Op != OpDelete {
		return c, fmt.Errorf("unknown op %q", c.Op)
	}
	return c, nil
}
