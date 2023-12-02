package pqlite

import (
	"context"
	"fmt"
)

// Notification represents a notification received from the database sent by
// a NOTIFY command.
type Notification struct {
	PID     int    // backend pid that sent the notification
	Channel []byte // channel name
	Payload []byte // notification payload
}

// NotificationListener listens for notifications sent by the database via
// NOTIFY commands. It only receives notifications on channels that have
// been explicitly LISTENed to using the Listen method.
type NotificationListener struct {
	c *Conn
}

func DialListener(ctx context.Context, dsnOrURL string) (*NotificationListener, error) {
	c, err := Dial(ctx, dsnOrURL)
	if err != nil {
		return nil, err
	}
	return &NotificationListener{c: c}, nil
}

func (l *NotificationListener) Listen(ctx context.Context, channel string) error {
	_, err := l.c.Exec(ctx, "LISTEN "+channel)
	return err
}

func (l *NotificationListener) Notifications(ctx context.Context) iter_Seq2[Notification, error] {
	return func(yield func(Notification, error) bool) {
		var zero Notification
		c := l.c.c
		for {
			h, err := c.ReadHeader(ctx)
			if err != nil {
				yield(zero, err)
				return
			}
			switch h.Type() {
			case 'A':
				m, err := c.ReadNotification(ctx, h)
				if err != nil {
					yield(zero, err)
					return
				}
				n := Notification{
					PID:     int(m.PID),
					Channel: m.Channel,
					Payload: m.Payload,
				}
				if !yield(n, nil) {
					return
				}
			case 'E':
				m, err := c.ReadError(ctx, h)
				if err != nil {
					yield(zero, err)
					return
				}
				yield(zero, &m.ErrorFields)
			default:
				yield(zero, fmt.Errorf("unexpected message type %q", h.Type()))
				return
			}
		}
	}
}

func (l *NotificationListener) Close() error {
	return l.c.Close()
}
