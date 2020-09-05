package sql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
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

// TODO: probably this should be a part of schema adapter, it is a temporary shortcut
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

type BinlogSubscriberConfig struct {
	ConsumerGroup string

	// ResendInterval is the time to wait before resending a nacked message.
	// Must be non-negative. Defaults to 1s.
	ResendInterval time.Duration

	// SchemaAdapter provides the schema-dependent queries and arguments for them, based on topic/message etc.
	SchemaAdapter ExtendedSchemaAdapter

	// OffsetsAdapter provides mechanism for saving acks and offsets of consumers.
	OffsetsAdapter OffsetsAdapter

	// InitializeSchema option enables initializing schema on making subscription.
	InitializeSchema bool

	// ColumnsMapping provides mechanism for mapping columns in tables to message.
	ColumnsMapping ColumnsMapping

	// Database provides mechanism for passing credentials to the database.
	Database Database
}

func (c *BinlogSubscriberConfig) setDefaults() {
	if c.ResendInterval == 0 {
		c.ResendInterval = time.Second
	}
	if c.Database.Host == "" {
		c.Database.Host = "localhost"
	}
	if c.Database.Port == "" {
		c.Database.Port = "3306"
	}
	if c.Database.User == "" {
		c.Database.User = "root"
	}
	if c.Database.Name == "" {
		c.Database.Name = "watermill"
	}
	if c.Database.Flavour == "" {
		c.Database.Flavour = mysql.MySQLFlavor
	}
	if c.ColumnsMapping.UUID == "" {
		c.ColumnsMapping.UUID = "uuid"
	}
	if c.ColumnsMapping.Payload == "" {
		c.ColumnsMapping.Payload = "payload"
	}
	if c.ColumnsMapping.Metadata == "" {
		c.ColumnsMapping.Metadata = "metadata"
	}
	if c.ColumnsMapping.Offset == "" {
		c.ColumnsMapping.Offset = "offset"
	}
}

func (c BinlogSubscriberConfig) validate() error {
	if c.ResendInterval <= 0 {
		return errors.New("resend interval must be a positive duration")
	}
	if c.SchemaAdapter == nil {
		return errors.New("schema adapter is nil")
	}
	if c.OffsetsAdapter == nil {
		return errors.New("offsets adapter is nil")
	}
	if c.Database.Flavour != mysql.MySQLFlavor && c.Database.Flavour != mysql.MariaDBFlavor {
		return errors.New("database config incorrect flavour value")
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
	// TODO: probably it should be removed, and the db connection should be setup based on configuration
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
		return nil, ErrBinlogSubscriberClosed
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

func (s *BinlogSubscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return nil
}

func (s *BinlogSubscriber) consume(ctx context.Context, topic string, out chan *message.Message) {
	defer s.subscribeWg.Done()

	logger := s.logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": s.config.ConsumerGroup,
	})

	tableName := s.config.SchemaAdapter.MessagesTable(topic)
	trimmedTableName := strings.Trim(tableName, "`")

	subscriptionCanal, err := s.getCanal(trimmedTableName)
	if err != nil {
		logger.Error("Error creating canal", err, nil)
		return
	}

	table, err := subscriptionCanal.GetTable(s.config.Database.Name, trimmedTableName)
	if err != nil {
		logger.Error("Error getting getting table", err, watermill.LogFields{
			"database": s.config.Database.Name,
			"table":    tableName,
		})
		return
	}

	sync := newBinlogSync(
		table,
		columnsIndexesMapping{
			offset:   table.FindColumn(s.config.ColumnsMapping.Offset),
			uuid:     table.FindColumn(s.config.ColumnsMapping.UUID),
			metadata: table.FindColumn(s.config.ColumnsMapping.Metadata),
			payload:  table.FindColumn(s.config.ColumnsMapping.Payload),
		},
	)

	subscriptionCanal.SetEventHandler(sync)

	go func() {
		select {
		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			subscriptionCanal.Close()

		case <-ctx.Done():
			logger.Info("Stopping consume, context canceled", nil)
			subscriptionCanal.Close()
		}
	}()

	// TODO: checkout if there might be a translation position - offset
	position, err := getZeroPosition(subscriptionCanal)
	if err != nil {
		logger.Error("Error getting zero position", err, nil)
		return
	}

	go func() {
		err = subscriptionCanal.RunFrom(position)
		if err != nil {
			logger.Error("Error starting sync", err, nil)
			return
		}
	}()

	for {
		select {
		case row := <-sync.RowsStream():
			// check offset
			// if offset incorrect skip
			// if offset ok
			//
			//row.Offset()
			err = s.process(ctx, row, topic, out, logger)
			if err != nil {
				logger.Error("Error processing row", err, nil)
				return
			}
		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return
		}
	}
}

func (s *BinlogSubscriber) process(
	ctx context.Context,
	r Row,
	topic string,
	out chan *message.Message,
	logger watermill.LoggerAdapter) error {
	txOptions := &sql.TxOptions{
		// TODO: check if it should be modified
		Isolation: sql.LevelRepeatableRead,
	}
	tx, err := s.db.BeginTx(ctx, txOptions)
	if err != nil {
		return errors.Wrap(err, "could not begin tx for querying")
	}

	defer func() {
		if err != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				logger.Error("could not rollback tx for processing message", rollbackErr, nil)
			}
		} else {
			commitErr := tx.Commit()
			if commitErr != nil {
				logger.Error("could not commit tx for processing message", commitErr, nil)
			}
		}
	}()

	var nextOffset int64
	nextOffsetQuery, nextOffsetQueryArgs := s.config.OffsetsAdapter.NextOffsetQuery(topic, s.config.ConsumerGroup)
	nextOffsetRow := tx.QueryRow(nextOffsetQuery, nextOffsetQueryArgs...)
	err = nextOffsetRow.Scan(&nextOffset)
	if err != nil {
		return errors.Wrap(err, "cannot get next offset")
	}

	if r.Offset() <= nextOffset {
		return tx.Rollback()
	}

	consumedQuery, consumedArgs := s.config.OffsetsAdapter.ConsumedMessageQuery(
		topic,
		int(r.offset),
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
			return errors.Wrap(err, "cannot send consumed query")
		}
	}

	msg, err := r.ToMessage()
	if err != nil {
		return errors.Wrap(err, "cannot map row to message")
	}

	acked := s.sendMessage(ctx, msg, out, logger)
	if acked {
		ackQuery, ackArgs := s.config.OffsetsAdapter.AckMessageQuery(topic, int(r.offset), s.config.ConsumerGroup)

		logger.Trace("Executing ack message query", watermill.LogFields{
			"query":      ackQuery,
			"query_args": sqlArgsToLog(ackArgs),
		})

		result, err := tx.ExecContext(ctx, ackQuery, ackArgs...)
		if err != nil {
			return errors.Wrap(err, "could not get args for acking the message")
		}

		rowsAffected, _ := result.RowsAffected()

		logger.Trace("Executed ack message query", watermill.LogFields{
			"rows_affected": rowsAffected,
		})
	}

	return nil
}

// sendMessages sends messages on the output channel.
func (s *BinlogSubscriber) sendMessage(
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

func (s BinlogSubscriber) getCanal(table string) (*canal.Canal, error) {
	dbConfig := s.config.Database

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", dbConfig.Host, dbConfig.Port)
	cfg.User = dbConfig.User
	cfg.Password = dbConfig.Password
	cfg.Flavor = dbConfig.Flavour
	cfg.Dump.TableDB = dbConfig.Name
	cfg.Dump.Tables = []string{table}
	cfg.Dump.ExecutionPath = ""

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

type ExtendedSchemaAdapter interface {
	SchemaAdapter
	MessagesTable(topic string) string
}

func getZeroPosition(c *canal.Canal) (mysql.Position, error) {
	rr, err := c.Execute("SHOW BINARY LOGS")
	if err != nil {
		return mysql.Position{}, err
	}

	name, err := rr.GetString(0, 0)
	if err != nil {
		return mysql.Position{}, err
	}
	return mysql.Position{Name: name, Pos: uint32(0)}, nil
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
}

func newRow(offset int64, uuid []byte, metadata []byte, payload []byte) Row {
	return Row{offset: offset, uuid: uuid, metadata: metadata, payload: payload}
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
