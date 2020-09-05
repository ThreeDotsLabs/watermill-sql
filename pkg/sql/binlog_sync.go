package sql

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"strings"
)

const mysqlDateFormat = "2006-01-02"

type binlogSync struct {
	table   *schema.Table
	mapping columnsIndexesMapping
	logger  watermill.LoggerAdapter
	// this gonna work because it will be either written or read
	position   mysql.Position
	rowsStream chan Row
}

func (b binlogSync) RowsStream() <-chan Row {
	return b.rowsStream
}

func newBinlogSync(
	table *schema.Table,
	mapping columnsIndexesMapping,
	logger watermill.LoggerAdapter,
) *binlogSync {
	return &binlogSync{
		table:      table,
		mapping:    mapping,
		logger:     logger,
		rowsStream: make(chan Row),
	}
}

func (b binlogSync) OnRotate(_ *replication.RotateEvent) error {
	return nil
}

func (b binlogSync) OnTableChanged(_ string, _ string) error {
	return nil
}

func (b binlogSync) OnDDL(_ mysql.Position, _ *replication.QueryEvent) error {
	return nil
}

func (b binlogSync) OnRow(e *canal.RowsEvent) error {
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
			b.rowsStream <- b.mapRow(e.Table, e.Rows[i])
		}
	case canal.UpdateAction, canal.DeleteAction:
	default:
		fmt.Printf("Unknown action")
	}
	return nil
}

func (b binlogSync) OnXID(_ mysql.Position) error {
	return nil
}

func (b binlogSync) OnGTID(_ mysql.GTIDSet) error {
	return nil
}

func (b *binlogSync) OnPosSynced(position mysql.Position, _ mysql.GTIDSet, _ bool) error {
	b.position = position
	return nil
}

func (b binlogSync) String() string {
	return "binlogSync"
}

func (b binlogSync) mapRow(table *schema.Table, r []interface{}) Row {
	mapping := b.mapping

	payload, err := getBytes(&table.Columns[mapping.payload], r[mapping.payload])
	if err != nil {
		// TODO: fix me
		fmt.Println(err)
	}

	uuid, err := getBytes(&table.Columns[mapping.uuid], r[mapping.uuid])
	if err != nil {
		// TODO: fix me
		fmt.Println(err)
	}

	metadata, err := getBytes(&table.Columns[mapping.metadata], r[mapping.metadata])
	if err != nil {
		// TODO: fix me
		fmt.Println(err)
	}

	offset, err := getNumberValue(&table.Columns[mapping.offset], r[mapping.offset])
	if err != nil {
		// TODO: fix me
		fmt.Println(err)
	}

	return newRow(offset, uuid, metadata, payload, b.position)
}

func getNumberValue(column *schema.TableColumn, value interface{}) (int64, error) {
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

/*
TYPE_NUMBER    = iota + 1 // tinyint, smallint, int, bigint, year
	TYPE_FLOAT                // float, double
	TYPE_ENUM                 // enum
	TYPE_SET                  // set
	TYPE_STRING               // char, varchar, etc.
	TYPE_DATETIME             // datetime
	TYPE_TIMESTAMP            // timestamp
	TYPE_DATE                 // date
	TYPE_TIME                 // time
	TYPE_BIT                  // bit
	TYPE_JSON                 // json
	TYPE_DECIMAL              // decimal
	TYPE_MEDIUM_INT
	TYPE_BINARY               // binary, varbinary
	TYPE_POINT                // coordinates
*/
