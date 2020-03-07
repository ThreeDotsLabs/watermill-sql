package sql

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/lib/pq"
)

const (
	metadataPIDKey     = "_metadata_postgres_PID"
	metadataChannelKey = "_metadata_postgres_PID"
)

type PostgresListenUnmarshaler interface {
	Unmarshal(notification *pq.Notification) (*message.Message, error)
}

type PostgresNotifyMarshaler interface {
	Marshal(msg *message.Message) (string, error)
}

// DefaultPostgresListenUnmarshaler unpacks the metadata and payload from a base64-encoded gob in Notification's Extra.
// It's compatible with DefaultPostgresNotifyMarshaler.
type DefaultPostgresListenUnmarshaler struct{}

func (u DefaultPostgresListenUnmarshaler) Unmarshal(notification *pq.Notification) (*message.Message, error) {
	b, err := base64.StdEncoding.DecodeString(notification.Extra)
	if err != nil {
		return nil, errors.Wrap(err, "cannot base64-decode the notification")
	}

	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)

	var msg *message.Message
	err = dec.Decode(msg)
	if err != nil {
		return nil, errors.Wrap(err, "error decoding message from gob")
	}

	msg.Metadata.Set(metadataPIDKey, fmt.Sprintf("%d", notification.BePid))
	msg.Metadata.Set(metadataChannelKey, notification.Channel)

	return msg, nil
}

// DefaultPostgresNotifyMarshaler packs the metadata and payload into a base64-encoded gob.
// It's compatible with DefaultPostgresNotifyMarshaler.
type DefaultPostgresNotifyMarshaler struct{}

func (m DefaultPostgresNotifyMarshaler) Marshal(msg *message.Message) (string, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	err := enc.Encode(msg)
	if err != nil {
		return "", errors.Wrap(err, "cannot gob encode the message")
	}

	if buf.Len() > 8*(2<<20) {
		return "", errors.New("message size exceeds 8 MB allowed by PostgreSQL")
	}

	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}
