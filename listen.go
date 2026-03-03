package pqlite

import (
	"context"
	"fmt"
	"iter"
)

// Notification represents a notification received from the database sent by
// a NOTIFY command.
type Notification struct {
	PID     int    // backend pid that sent the notification
	Channel []byte // channel name
	Payload []byte // notification payload
}

// Listener receives PostgreSQL LISTEN/NOTIFY messages.
//
// It only yields notifications for channels registered with [Listener.Listen].
type Listener struct {
	c *Conn
}

// OpenListener opens a dedicated connection for listening for notifications
// on channels using PostgreSQL's LISTEN/NOTIFY feature.
func OpenListener(ctx context.Context, dsnOrURL string, cfg *Config) (*Listener, error) {
	cn, err := Open(ctx, dsnOrURL, cfg)
	if err != nil {
		return nil, err
	}
	return &Listener{c: cn}, nil
}

// Listen registers channel with LISTEN.
//
// channel must be a valid PostgreSQL identifier. Listen builds the SQL text
// directly because LISTEN does not support bind parameters.
func (l *Listener) Listen(ctx context.Context, channel string) error {
	// NOTE: Using bind parameters with LISTEN is illegal, so
	// we have to build the query here.
	//
	// TODO: validate channel name (pg doesn't provide nice
	// feedback on invalid channel names).
	return l.c.Exec(ctx, "LISTEN "+channel)
}

// Notifications yields notifications until ctx is canceled or an error occurs.
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
				m, err := c.ReadNotification()
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
			default:
				yield(zero, fmt.Errorf("unexpected message type %q", h.Type()))
				return
			}
		}
	}
}

// Close closes the underlying dedicated connection.
func (l *Listener) Close() error {
	return l.c.Close()
}
