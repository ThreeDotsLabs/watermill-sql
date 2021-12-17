package sql

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
	"strings"
)

type mysqlBinlogSync struct {
	table      *schema.Table
	mapping    columnsIndexesMapping
	logger     watermill.LoggerAdapter
	position   mysql.Position
	rowsStream chan Row
}

func newBinlogSync(
	table *schema.Table,
	mapping columnsIndexesMapping,
	logger watermill.LoggerAdapter,
) *mysqlBinlogSync {
	return &mysqlBinlogSync{
		table:      table,
		mapping:    mapping,
		logger:     logger.With(watermill.LogFields{"table_to_sync": table.Name}),
		rowsStream: make(chan Row),
	}
}

func (b mysqlBinlogSync) RowsStream() <-chan Row {
	return b.rowsStream
}

func (b mysqlBinlogSync) OnRotate(_ *replication.RotateEvent) error {
	return nil
}

func (b mysqlBinlogSync) OnTableChanged(_ string, _ string) error {
	// it should either stop or reinitialize
	return nil
}

func (b mysqlBinlogSync) OnDDL(_ mysql.Position, _ *replication.QueryEvent) error {
	return nil
}

func (b mysqlBinlogSync) OnRow(e *canal.RowsEvent) error {
	var firstValueIndex = 0
	var stepLength = 1

	if b.table.Name != e.Table.Name {
		return nil
	}

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
			b.rowsStream <- row
		}
	case canal.UpdateAction, canal.DeleteAction:
	default:
		b.logger.Info("Unknown action", nil)
	}
	return nil
}

func (b mysqlBinlogSync) OnXID(_ mysql.Position) error {
	return nil
}

func (b mysqlBinlogSync) OnGTID(_ mysql.GTIDSet) error {
	return nil
}

func (b *mysqlBinlogSync) OnPosSynced(position mysql.Position, _ mysql.GTIDSet, _ bool) error {
	b.position = position
	return nil
}

func (b mysqlBinlogSync) String() string {
	return "mysqlBinlogSync_" + b.table.Name
}

func (b mysqlBinlogSync) mapRow(table *schema.Table, r []interface{}) (Row, error) {
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

	return newRow(offset, uuid, metadata, payload, b.position), nil
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
	}

	return []byte{}, fmt.Errorf("could not resolve value for column of type %d", column.Type)
}
