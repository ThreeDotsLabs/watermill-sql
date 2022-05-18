package streamer

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
	"strings"
	"sync"
)

type BinlogEventHandler struct {
	mapping              columnsIndexesMapping
	logger               watermill.LoggerAdapter
	latestPositionToSave mysql.Position
	rowsCh               chan Row
	mu                   sync.RWMutex
	closed               bool
}

func (b *BinlogEventHandler) RowsCh() <-chan Row {
	return b.rowsCh
}

func NewBinlogEventHandler(
	mapping columnsIndexesMapping,
	logger watermill.LoggerAdapter,
) *BinlogEventHandler {
	return &BinlogEventHandler{
		mapping: mapping,
		logger:  logger,
		rowsCh:  make(chan Row),
		closed:  false,
		mu:      sync.RWMutex{},
	}
}

func (b *BinlogEventHandler) OnRotate(e *replication.RotateEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.latestPositionToSave = mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	return nil
}

func (b *BinlogEventHandler) OnTableChanged(_ string, _ string) error {
	// recalculate column's indices

	return nil
}

func (b *BinlogEventHandler) OnDDL(p mysql.Position, _ *replication.QueryEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.latestPositionToSave = p

	return nil
}

func (b *BinlogEventHandler) OnXID(p mysql.Position) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.latestPositionToSave = p

	return nil
}

func (b *BinlogEventHandler) OnRow(e *canal.RowsEvent) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var firstValueIndex = 0
	var stepLength = 1

	if e.Action == canal.UpdateAction {
		firstValueIndex = 1
		stepLength = 2
	}

	switch e.Action {
	case canal.InsertAction:
		for i := firstValueIndex; i < len(e.Rows); i += stepLength {
			row, err := b.mapRow(e.Table, e.Rows[i])
			if err != nil {
				return err
			}

			b.rowsCh <- row
		}
	case canal.UpdateAction, canal.DeleteAction:
	default:
		b.logger.Info("Unknown action", nil)
	}
	return nil
}

func (b *BinlogEventHandler) OnGTID(_ mysql.GTIDSet) error {
	return nil
}

func (b *BinlogEventHandler) OnPosSynced(_ mysql.Position, _ mysql.GTIDSet, _ bool) error {
	return nil
}

func (b *BinlogEventHandler) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.closed = true
	close(b.rowsCh)

	return
}

func (b *BinlogEventHandler) String() string {
	return "mysqlBinlogSync"
}

func (b *BinlogEventHandler) mapRow(table *schema.Table, r []interface{}) (Row, error) {
	mapping := b.mapping

	payload, err := getBytes(&table.Columns[mapping.payload], r[mapping.payload])
	if err != nil {
		return Row{}, errors.Wrap(err, "could not get bytes for payload")
	}

	uuid, err := getBytes(&table.Columns[mapping.uuid], r[mapping.uuid])
	if err != nil {
		return Row{}, errors.Wrap(err, "could not get bytes for uuid")
	}

	metadata, err := getBytes(&table.Columns[mapping.metadata], r[mapping.metadata])
	if err != nil {
		return Row{}, errors.Wrap(err, "could not get bytes for metadata")
	}

	offset, err := getNumber(&table.Columns[mapping.offset], r[mapping.offset])
	if err != nil {
		return Row{}, errors.Wrap(err, "could not get number for offset")
	}

	return newRow(offset, uuid, metadata, payload, b.latestPositionToSave), nil
}

func getNumber(column *schema.TableColumn, value interface{}) (int64, error) {
	if column.Type != schema.TYPE_NUMBER {
		return 0, errors.New("column is not of type number")
	}

	switch value.(type) {
	case int8:
		return int64(value.(int8)), nil
	case int32:
		return int64(value.(int32)), nil
	case int64:
		return value.(int64), nil
	case int:
		return int64(value.(int)), nil
	case uint8:
		return int64(value.(uint8)), nil
	case uint16:
		return int64(value.(uint16)), nil
	case uint32:
		return int64(value.(uint32)), nil
	case uint64:
		return int64(value.(uint64)), nil
	case uint:
		return int64(value.(uint)), nil
	}

	return 0, errors.New("unsupported type for number column")
}

// based on https://github.com/siddontang/go-mysql-elasticsearch/blob/fe261969558bf79dffa46d37d2b95f62d65502a1/river/sync.go#L273
func getBytes(column *schema.TableColumn, value interface{}) ([]byte, error) {
	switch column.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(column.EnumValues)) {
				// we insert invalid enum value before, so return empty
				return []byte{}, fmt.Errorf("invalid binlog enum index %d, for enum %v", eNum, column.EnumValues)
			}

			return []byte(column.EnumValues[eNum]), nil
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(column.SetValues))
			for i, s := range column.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return []byte(strings.Join(sets, ",")), nil
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			return []byte(value), nil
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return value, nil
		case string:
			return []byte(value), nil
		}
	case schema.TYPE_JSON:
		switch v := value.(type) {
		case nil:
			return []byte{}, nil
		case string:
			return []byte(v), nil
		case []byte:
			return v, nil
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP, schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			return []byte(v), nil
		}
	case schema.TYPE_BINARY:
		switch v := value.(type) {
		case string:
			return []byte(v), nil
		case []byte:
			return v, nil
		default:
			if v == nil {
				return []byte{}, nil
			}
			return []byte{}, fmt.Errorf("could not resolve value for column of type %d", column.Type)
		}
	}

	return []byte{}, fmt.Errorf("could not resolve value for column of type %d", column.Type)
}
