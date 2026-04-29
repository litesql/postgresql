package replication

import (
	"fmt"
	"time"
)

type Change struct {
	ServerTime   time.Time `json:"servertime"`
	Kind         string    `json:"kind"`
	Schema       string    `json:"schema"`
	Table        string    `json:"table"`
	ColumnNames  []string  `json:"columnnames"`
	ColumnValues []any     `json:"columnvalues"`
	OldKeys      struct {
		KeyNames  []string `json:"keynames,omitempty"`
		KeyValues []any    `json:"keyvalues,omitempty"`
	} `json:"oldkeys"`
	SQL string `json:"sql"`
}

func (c Change) RevertSQL() (string, []any) {
	// generate SQL to revert the change based on the Kind field
	// for example, if Kind is "insert", generate a DELETE statement
	// if Kind is "update", generate an UPDATE statement to revert the changes
	// if Kind is "delete", generate an INSERT statement to restore the deleted row

	switch c.Kind {
	case "INSERT":
		var args []any
		// generate DELETE statement using c.Schema, c.Table, c.ColumnNames and args
		sql := fmt.Sprintf("DELETE FROM %s.%s WHERE ", c.Schema, c.Table)
		for i, col := range c.ColumnNames {
			if i > 0 {
				sql += " AND "
			}
			if c.ColumnValues[i] == nil {
				sql += fmt.Sprintf("%s IS NULL", col)
			} else {
				args = append(args, c.ColumnValues[i])
				sql += fmt.Sprintf("%s = $%d", col, len(args))
			}
		}
		return sql, args
	case "UPDATE":
		var args []any
		args = append(args, c.OldKeys.KeyValues...)
		// generate UPDATE statement to revert changes using c.Schema, c.Table, c.ColumnNames and args
		sql := fmt.Sprintf("UPDATE %s.%s SET ", c.Schema, c.Table)
		for i, col := range c.ColumnNames {
			if i > 0 {
				sql += ", "
			}
			sql += fmt.Sprintf("%s = $%d", col, i+1)
		}
		sql += " WHERE "
		for i, key := range c.OldKeys.KeyNames {
			if i > 0 {
				sql += " AND "
			}
			if c.OldKeys.KeyValues[i] == nil {
				sql += fmt.Sprintf("%s IS NULL", key)
			} else {
				args = append(args, c.ColumnValues[i])
				sql += fmt.Sprintf("%s = $%d", key, len(args))
			}
		}
		return sql, args
	case "DELETE":
		// generate INSERT statement to restore deleted row
		sql := fmt.Sprintf("INSERT INTO %s.%s (", c.Schema, c.Table)
		for i, col := range c.ColumnNames {
			if i > 0 {
				sql += ", "
			}
			sql += col
		}
		sql += ") VALUES ("
		for i := 0; i < len(c.ColumnNames); i++ {
			if i > 0 {
				sql += ", "
			}
			sql += fmt.Sprintf("$%d", i+1)
		}
		sql += ")"
		return sql, c.ColumnValues
	}
	return "", nil
}
