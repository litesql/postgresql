package replication

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func Identify(dsn string) string {
	pgConfig, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	pgConfig.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	return fmt.Sprintf(`{"systemid": "%s", "timeline": %d, "xlogpos": "%s", "dbname": "%s"}`, sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)
}

func CreateSlot(dsn, slot string) string {
	pgConfig, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	pgConfig.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		return fmt.Sprintf(`{"error": %s}`, err.Error())
	}
	defer conn.Close(context.Background())

	res, err := pglogrepl.CreateReplicationSlot(context.Background(), conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
		Mode:      pglogrepl.LogicalReplication,
	})
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	return fmt.Sprintf(`{"slot": "%s", "plugin": "%s", "snapshot": "%s", "consistentPoint": "%s"}`, res.SlotName, res.OutputPlugin, res.SnapshotName, res.ConsistentPoint)
}

func DropSlot(dsn, slot string) string {
	pgConfig, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	pgConfig.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	defer conn.Close(context.Background())

	err = pglogrepl.DropReplicationSlot(context.Background(), conn, slot, pglogrepl.DropReplicationSlotOptions{
		Wait: true,
	})
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	return fmt.Sprintf(`{"status": "slot %s dropped successfully"}`, slot)
}

type publicationTablesResult struct {
	Publication any
	Schema      any
	Table       any
	Attributes  any
	RowFilter   any
}

func PublicationTables(dsn string) string {
	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	pgConfig.RuntimeParams["replication"] = "database"
	conn, err := pgx.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), "SELECT pubname, schemaname, tablename, attnames, rowfilter FROM pg_publication_tables")
	if err != nil {
		return fmt.Sprintf(`{"error": "%s"}`, err.Error())
	}
	defer rows.Close()
	result := make([]publicationTablesResult, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Sprintf(`{"error": "%s"}`, err.Error())
		}
		result = append(result, publicationTablesResult{
			Publication: values[0],
			Schema:      values[1],
			Table:       values[2],
			Attributes:  values[3],
			RowFilter:   values[4],
		})
	}
	jsonBytes, _ := json.Marshal(result)
	return string(jsonBytes)
}
