package sql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var (
	ErrBinlogSubscriberClosed = errors.New("binlog subscriber is closed")
)

type BinlogSubscriberConfig struct {
	ConsumerGroup string

	// ResendInterval is the time to wait before resending a nacked message.
	// Must be non-negative. Defaults to 1s.
	ResendInterval time.Duration

	// RetryInterval is the time to wait before resuming querying for messages after an error.
	// Must be non-negative. Defaults to 1s.
	RetryInterval time.Duration

	// SchemaAdapter provides the schema-dependent queries and arguments for them, based on topic/message etc.
	SchemaAdapter SchemaAdapter

	// OffsetsAdapter provides mechanism for saving acks and offsets of consumers.
	OffsetsAdapter OffsetsAdapter

	// InitializeSchema option enables initializing schema on making subscription.
	InitializeSchema bool

	// TODO: improve this config construction
	Host     string
	Port     string
	User     string
	Password string
	Flavour  string
}

func (c *BinlogSubscriberConfig) setDefaults() {
	if c.ResendInterval == 0 {
		c.ResendInterval = time.Second
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = time.Second
	}
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port == "" {
		c.Port = "3306"
	}
	if c.User == "" {
		c.User = "root"
	}
	if c.Flavour == "" {
		c.Flavour = "mysql"
	}
}

func (c BinlogSubscriberConfig) validate() error {
	if c.ResendInterval <= 0 {
		return errors.New("resend interval must be a positive duration")
	}
	if c.RetryInterval <= 0 {
		return errors.New("resend interval must be a positive duration")
	}
	if c.SchemaAdapter == nil {
		return errors.New("schema adapter is nil")
	}
	if c.OffsetsAdapter == nil {
		return errors.New("offsets adapter is nil")
	}

	return nil
}

// source https://github.com/samsarahq/thunder/blob/919f3b6eccbda64164656a2c890ae31de3a34ed5/livesql/binlog.go#L48
// checkVariable verifies database configuration value
func checkVariable(conn beginner, variable, expected string) error {
	row := conn.QueryRow(fmt.Sprintf(`SHOW GLOBAL VARIABLES LIKE "%s"`, variable))
	var value string
	var ignored interface{}
	if err := row.Scan(&ignored, &value); err != nil {
		return fmt.Errorf("error reading MySQL variable %s: %s", variable, err)
	}

	if !strings.EqualFold(value, expected) {
		return fmt.Errorf("expected MySQL variable %s to be %s, but got %s", variable, expected, value)
	}

	return nil
}

// BinlogSubscriber reads binary log files that contain information about data modifications made to a MySQL server instance.
// The rows are unmarshalled into Watermill messages.
type BinlogSubscriber struct {
	consumerIdBytes  []byte
	consumerIdString string

	db     beginner
	config BinlogSubscriberConfig

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool

	logger watermill.LoggerAdapter
}

func NewBinlogSubscriber(db beginner, config BinlogSubscriberConfig, logger watermill.LoggerAdapter) (*BinlogSubscriber, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	// TODO: check if below requirements have to be strict or they can be relaxed to a warning
	if err := checkVariable(db, "binlog_format", "ROW"); err != nil {
		return nil, errors.Wrap(err, "invalid database configuration")
	}
	if err := checkVariable(db, "binlog_row_image", "FULL"); err != nil {
		return nil, errors.Wrap(err, "invalid database configuration")
	}

	config.setDefaults()
	err := config.validate()
	if err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	idBytes, idStr, err := newSubscriberID()
	if err != nil {
		return &BinlogSubscriber{}, errors.Wrap(err, "cannot generate subscriber id")
	}
	logger = logger.With(watermill.LogFields{"subscriber_id": idStr})

	sub := &BinlogSubscriber{
		consumerIdBytes:  idBytes,
		consumerIdString: idStr,

		db:     db,
		config: config,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),

		logger: logger,
	}

	return sub, nil
}

func (s *BinlogSubscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	if err = validateTopicName(topic); err != nil {
		return nil, err
	}

	if s.config.InitializeSchema {
		if err := s.SubscribeInitialize(topic); err != nil {
			return nil, err
		}
	}

	// the information about closing the subscriber is propagated through ctx
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, topic, out)
		close(out)
		cancel()
	}()

	return out, nil
}

func (s *BinlogSubscriber) consume(ctx context.Context, topic string, out chan *message.Message) {
	defer s.subscribeWg.Done()

	logger := s.logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": s.config.ConsumerGroup,
	})

	canal, err := newCanal(
		s.config.Host,
		s.config.Port,
		s.config.User,
		s.config.Password,
		s.config.Flavour,
	)
	if err != nil {
		logger.Error("Error creating canal", err, nil)
		return
	}
	rr, err := canal.Execute("SHOW BINARY LOGS")
	if err != nil {
		logger.Error("Error reading binary log files", err, nil)
		return
	}

	name, err := rr.GetString(0, 0)
	if err != nil {
		logger.Error("Error reading first binary log file", err, nil)
		return
	}

	zeroPosition := mysql.Position{Name: name, Pos: uint32(0)}

	canal.SetEventHandler(&binlogHandler{})

	go func() {
		select {
		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			canal.Close()

		case <-ctx.Done():
			logger.Info("Stopping consume, context canceled", nil)
			canal.Close()
		}
	}()

	err = canal.RunFrom(zeroPosition)
	if err != nil {
		logger.Error("Error starting sync", err, nil)
		return
	}

	// here probably retry is missing
	for {

		messageUUID, err := s.query(ctx, topic, out, logger)
		if err != nil && isDeadlock(err) {
			logger.Debug("Deadlock during querying message, trying again", watermill.LogFields{
				"err":          err.Error(),
				"message_uuid": messageUUID,
			})
		} else if err != nil {
			logger.Error("Error querying for message", err, nil)
			time.Sleep(s.config.RetryInterval)
		}
	}
}

func newCanal(
	host string,
	port string,
	user string,
	password string,
	flavor string,
) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.User = user
	cfg.Password = password
	cfg.Flavor = flavor

	serverID, err := getRandUint32()
	if err != nil {
		return nil, errors.Wrap(err, "could not generate random uint32")
	}

	cfg.ServerID = serverID
	cfg.Dump.ExecutionPath = ""

	return canal.NewCanal(cfg)
}

func getRandUint32() (uint32, error) {
	slaveId := make([]byte, 4)
	_, err := rand.Read(slaveId)

	return binary.LittleEndian.Uint32(slaveId), err
}

func (s *Subscriber) query(
	ctx context.Context,
	topic string,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (messageUUID string, err error) {
	txOptions := &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	}
	tx, err := s.db.BeginTx(ctx, txOptions)
	if err != nil {
		return "", errors.Wrap(err, "could not begin tx for querying")
	}

	defer func() {
		if err != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				logger.Error("could not rollback tx for querying message", rollbackErr, nil)
			}
		} else {
			commitErr := tx.Commit()
			if commitErr != nil {
				logger.Error("could not commit tx for querying message", commitErr, nil)
			}
		}
	}()

	selectQuery, selectQueryArgs := s.config.SchemaAdapter.SelectQuery(
		topic,
		s.config.ConsumerGroup,
		s.config.OffsetsAdapter,
	)
	logger.Trace("Querying message", watermill.LogFields{
		"query":      selectQuery,
		"query_args": sqlArgsToLog(selectQueryArgs),
	})
	row := tx.QueryRowContext(ctx, selectQuery, selectQueryArgs...)

	offset, msg, err := s.config.SchemaAdapter.UnmarshalMessage(row)
	if errors.Cause(err) == sql.ErrNoRows {
		// wait until polling for the next message
		logger.Debug("No more messages, waiting until next query", watermill.LogFields{
			"wait_time": s.config.PollInterval,
		})
		time.Sleep(s.config.PollInterval)
		return "", nil
	} else if err != nil {
		return "", errors.Wrap(err, "could not unmarshal message from query")
	}

	logger = logger.With(watermill.LogFields{
		"msg_uuid": msg.UUID,
	})
	logger.Trace("Received message", nil)

	consumedQuery, consumedArgs := s.config.OffsetsAdapter.ConsumedMessageQuery(
		topic,
		offset,
		s.config.ConsumerGroup,
		s.consumerIdBytes,
	)
	if consumedQuery != "" {
		logger.Trace("Executing query to confirm message consumed", watermill.LogFields{
			"query":      consumedQuery,
			"query_args": sqlArgsToLog(consumedArgs),
		})

		_, err := tx.ExecContext(ctx, consumedQuery, consumedArgs...)
		if err != nil {
			return msg.UUID, errors.Wrap(err, "cannot send consumed query")
		}
	}

	acked := s.sendMessage(ctx, msg, out, logger)
	if acked {
		ackQuery, ackArgs := s.config.OffsetsAdapter.AckMessageQuery(topic, offset, s.config.ConsumerGroup)

		logger.Trace("Executing ack message query", watermill.LogFields{
			"query":      ackQuery,
			"query_args": sqlArgsToLog(ackArgs),
		})

		result, err := tx.ExecContext(ctx, ackQuery, ackArgs...)
		if err != nil {
			return msg.UUID, errors.Wrap(err, "could not get args for acking the message")
		}

		rowsAffected, _ := result.RowsAffected()

		logger.Trace("Executed ack message query", watermill.LogFields{
			"rows_affected": rowsAffected,
		})
	}

	return msg.UUID, nil
}

// sendMessages sends messages on the output channel.
func (s *Subscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (acked bool) {
	msgCtx, cancel := context.WithCancel(ctx)
	msg.SetContext(msgCtx)
	defer cancel()

ResendLoop:
	for {

		select {
		case out <- msg:

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}

		select {
		case <-msg.Acked():
			logger.Debug("Message acked by subscriber", nil)
			return true

		case <-msg.Nacked():
			//message nacked, try resending
			logger.Debug("Message nacked, resending", nil)
			msg = msg.Copy()

			if s.config.ResendInterval != 0 {
				time.Sleep(s.config.ResendInterval)
			}

			continue ResendLoop

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return nil
}

func (s *BinlogSubscriber) SubscribeInitialize(topic string) error {
	return initializeSchema(
		context.Background(),
		topic,
		s.logger,
		s.db,
		s.config.SchemaAdapter,
		s.config.OffsetsAdapter,
	)
}
