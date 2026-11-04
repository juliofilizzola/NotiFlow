package event

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Channel string

const (
	ChannelEmail Channel = "email"
	ChannelSMS   Channel = "sms"
	ChannelPush  Channel = "push"
)

const maxIDLength = 128

func (c Channel) Valid() bool {
	switch c {
	case ChannelEmail, ChannelSMS, ChannelPush:
		return true
	}
	return false
}

type Event struct {
	EventID    string         `json:"event_id"`
	UserID     string         `json:"user_id"`
	Type       string         `json:"type"`
	Channel    Channel        `json:"channel"`
	Data       map[string]any `json:"data,omitempty"`
	ReceivedAt time.Time      `json:"received_at"`
}

func (e Event) Validate() error {
	var errs []error
	errs = append(errs, requireID("event_id", e.EventID), requireID("user_id", e.UserID))
	if strings.TrimSpace(e.Type) == "" {
		errs = append(errs, errors.New("type is required"))
	}
	if !e.Channel.Valid() {
		errs = append(errs, fmt.Errorf("channel must be one of %q, %q, %q", ChannelEmail, ChannelSMS, ChannelPush))
	}
	return errors.Join(errs...)
}

func requireID(field, value string) error {
	switch {
	case strings.TrimSpace(value) == "":
		return fmt.Errorf("%s is required", field)
	case len(value) > maxIDLength:
		return fmt.Errorf("%s must be at most %d characters", field, maxIDLength)
	}
	return nil
}
