package postgres

import (
	"context"
	"fmt"
	"iter"

	"go4.org/mem"
)

// Notification represents a notification received from the database sent by
// a NOTIFY command.
type Notification struct {
	PID     int    // backend pid that sent the notification
	Channel mem.RO // channel name
	Payload mem.RO // notification payload
}

// Listener listens for notifications sent by the database via
// NOTIFY commands. It only receives notifications on channels that have
// been explicitly LISTENed to using the Listen method.
type Listener struct {
	c *Conn
}

func DialListener(ctx context.Context, dsnOrURL string, cfg *Config) (*Listener, error) {
	c, err := Dial(ctx, dsnOrURL, cfg)
	if err != nil {
		return nil, err
	}
	return &Listener{c: c}, nil
}

func (l *Listener) Listen(ctx context.Context, channel string) error {
	// NOTE: Using bind parameters with LISTEN is illegal, so
	// we have to build the query here.
	//
	// TODO: validate channel name (pg doesn't provide nice
	// feedback on invalid channel names).
	return l.c.Exec(ctx, "LISTEN "+channel)
}

func (l *Listener) Notifications(ctx context.Context) iter.Seq2[Notification, error] {
	return func(yield func(Notification, error) bool) {
		var zero Notification
		c := l.c.fc
		for {
			h, err := c.ReadHeader(ctx)
			if err != nil {
				yield(zero, err)
				return
			}
			switch h.Type() {
			case 'A':
				m, err := c.ReadNotification(ctx)
				if err != nil {
					yield(zero, err)
					return
				}
				n := Notification{
					PID:     int(m.PID),
					Channel: mem.B(m.Channel),
					Payload: mem.B(m.Payload),
				}
				if !yield(n, nil) {
					return
				}
			default:
				yield(zero, fmt.Errorf("unexpected message type %q", h.Type()))
				return
			}
		}
	}
}

func (l *Listener) Close() error {
	return l.c.Close()
}
