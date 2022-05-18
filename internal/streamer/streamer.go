package streamer

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	"sync"
)

type ColumnsMapping struct {
	UUID     string
	Offset   string
	Payload  string
	Metadata string
}

type Database struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	Flavour  string
}

type StreamsConfig struct {
	Table string
	Database
	ColumnsMapping
}

//TODO: add validation
func (c StreamsConfig) validate() error {
	return nil
}

// Streamer streams rows containing offset, published message and position
type Streamer struct {
	config StreamsConfig
	logger watermill.LoggerAdapter

	canal *canal.Canal

	subscribeWg *sync.WaitGroup
	closed      bool
	closing     chan struct{}

	err error
}

func (s *Streamer) Err() error {
	return s.err
}

func NewStreamer(
	config StreamsConfig,
	logger watermill.LoggerAdapter,
) (*Streamer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &Streamer{
		config:      config,
		logger:      logger,
		closed:      false,
		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),
	}, nil
}

func (s *Streamer) Stream(ctx context.Context, startPosition mysql.Position) (<-chan Row, error) {
	c, err := newCanal(s.config)
	if err != nil {
		// add some logging here
		return nil, err
	}

	s.canal = c

	table, err := s.canal.GetTable(s.config.Database.Name, s.config.Table)
	if err != nil {
		// add some logging here
		return nil, err
	}

	columnsMapping := columnsIndexesMapping{
		offset:   table.FindColumn(s.config.ColumnsMapping.Offset),
		uuid:     table.FindColumn(s.config.ColumnsMapping.UUID),
		metadata: table.FindColumn(s.config.ColumnsMapping.Metadata),
		payload:  table.FindColumn(s.config.ColumnsMapping.Payload),
	}

	handler := NewBinlogEventHandler(
		columnsMapping,
		s.logger,
	)
	s.canal.SetEventHandler(handler)

	s.subscribeWg.Add(1)
	go func() {
		defer s.subscribeWg.Done()

		err := s.canal.RunFrom(startPosition)
		if err != nil {
			s.logger.Error("Could not run canal from position", err, watermill.LogFields{
				"position": startPosition,
			})
			s.err = errors.Wrapf(err, "could not run canal from position '%v'", startPosition)
		}
	}()

	s.subscribeWg.Add(1)
	go func() {
		defer s.subscribeWg.Done()

		for {
			select {
			case <-s.closing:
				s.logger.Info("Closing canal", nil)
				s.canal.Close()
				handler.Close()
				return
			case <-ctx.Done():
				s.logger.Info("Closing canal", nil)
				s.canal.Close()
				handler.Close()
				return
			}
		}
	}()

	return handler.RowsCh(), nil
}

func (s *Streamer) Close() {
	if s.closed {
		return
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return
}

func newCanal(c StreamsConfig) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", c.Host, c.Port)
	cfg.User = c.User
	cfg.Password = c.Password
	cfg.Flavor = c.Flavour
	cfg.Dump.ExecutionPath = ""
	cfg.IncludeTableRegex = []string{c.Name + "." + c.Table}

	serverID, err := getRandUint32()
	if err != nil {
		return nil, errors.Wrap(err, "could not generate random uint32")
	}

	cfg.ServerID = serverID

	return canal.NewCanal(cfg)
}

func getRandUint32() (uint32, error) {
	slaveId := make([]byte, 4)
	_, err := rand.Read(slaveId)

	return binary.LittleEndian.Uint32(slaveId), err
}

type columnsIndexesMapping struct {
	offset   int
	uuid     int
	metadata int
	payload  int
}

type Row struct {
	offset   int64
	uuid     []byte
	metadata []byte
	payload  []byte
	// create a special type for this
	position mysql.Position
}

func newRow(offset int64, uuid []byte, metadata []byte, payload []byte, position mysql.Position) Row {
	return Row{offset: offset, uuid: uuid, metadata: metadata, payload: payload, position: position}
}

func (r Row) Position() mysql.Position {
	return r.position
}

func (r Row) Payload() []byte {
	return r.payload
}

func (r Row) Metadata() []byte {
	return r.metadata
}

func (r Row) Uuid() []byte {
	return r.uuid
}

func (r Row) Offset() int64 {
	return r.offset
}

func (r Row) ToMessage() (*message.Message, error) {
	msg := message.NewMessage(string(r.uuid), r.payload)

	if r.metadata != nil {
		err := json.Unmarshal(r.metadata, &msg.Metadata)
		if err != nil {
			return nil, errors.Wrap(err, "could not unmarshal metadata as JSON")
		}
	}

	return msg, nil
}
