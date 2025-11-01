package replication

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/kyleconroy/pgoutput"
)

var (
	set     = pgoutput.NewRelationSet(nil)
	changes = make([]Change, 0)
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
}

func pgOutputAdapter(fn HandleChanges) handler {
	return func(xld pglogrepl.XLogData) error {
		msg, err := pgoutput.Parse(xld.WALData)
		if err != nil {
			return err
		}
		switch v := msg.(type) {
		case pgoutput.Relation:
			set.Add(v)
			return nil
		case pgoutput.Insert:
			change, err := decode(v.RelationID, v.Row, nil)
			if err != nil {
				return err
			}
			change.Kind = "INSERT"
			change.ServerTime = xld.ServerTime
			changes = append(changes, change)
			return nil
		case pgoutput.Update:
			change, err := decode(v.RelationID, v.Row, v.OldRow)
			if err != nil {
				return err
			}
			change.Kind = "UPDATE"
			change.ServerTime = xld.ServerTime
			changes = append(changes, change)
			return nil
		case pgoutput.Delete:
			change, err := decode(v.RelationID, v.Row, nil)
			if err != nil {
				return err
			}
			change.Kind = "DELETE"
			change.ServerTime = xld.ServerTime
			changes = append(changes, change)
			return nil
		case pgoutput.Begin:
			changes = make([]Change, 0)
			return nil
		case pgoutput.Commit:
			if fn != nil {
				currentPosition := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				return fn(changes, currentPosition)
			}
			return nil
		}
		return nil
	}
}

func decode(relation uint32, row []pgoutput.Tuple, oldRow []pgoutput.Tuple) (Change, error) {
	values, err := set.Values(relation, row)
	if err != nil {
		return Change{}, fmt.Errorf("error parsing values: %s", err)
	}
	var c Change
	rel, ok := set.Get(relation)
	if !ok {
		return Change{}, fmt.Errorf("error relation not found")
	}
	c.Table = rel.Name
	c.Schema = rel.Namespace

	c.ColumnNames = make([]string, len(values))
	c.ColumnValues = make([]any, len(values))
	i := 0
	for name, value := range values {
		val := value.Get()
		c.ColumnNames[i] = name
		c.ColumnValues[i] = val
		i++
	}
	if oldRow != nil {
		oldValues, err := set.Values(relation, oldRow)
		if err != nil {
			return Change{}, fmt.Errorf("error parsing old values: %s", err)
		}
		c.OldKeys.KeyNames = make([]string, len(oldValues))
		c.OldKeys.KeyValues = make([]any, len(oldValues))
		i := 0
		for name, value := range oldValues {
			val := value.Get()
			c.OldKeys.KeyNames[i] = name
			c.OldKeys.KeyValues[i] = val
			i++
		}
	}
	return c, nil
}

func wal2JsonAdapter(fn HandleChanges) handler {
	return func(xld pglogrepl.XLogData) error {
		var changes struct {
			Data []Change `json:"change"`
		}
		err := json.Unmarshal(xld.WALData, &changes)
		if err != nil {
			return fmt.Errorf("error unmarshal handleWal2Json message: %w", err)
		}
		for i, c := range changes.Data {
			c.ServerTime = xld.ServerTime
			changes.Data[i] = c
		}
		if fn != nil {
			currentPosition := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			return fn(changes.Data, currentPosition)
		}
		return nil
	}
}
