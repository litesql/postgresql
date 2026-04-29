package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type SystemInfo struct {
	SystemID string `json:"systemid"`
	Timeline int32  `json:"timeline"`
	XLogPos  string `json:"xlogpos"`
	DBName   string `json:"dbname"`
}

func Identify(dsn string) (*SystemInfo, error) {
	pgConfig, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pgConfig.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		return nil, err
	}
	return &SystemInfo{
		SystemID: sysident.SystemID,
		Timeline: sysident.Timeline,
		XLogPos:  sysident.XLogPos.String(),
		DBName:   sysident.DBName,
	}, nil
}

type SlotInfo struct {
	Name         string `json:"name"`
	Plugin       string `json:"plugin"`
	Database     string `json:"database"`
	SnapshotName string `json:"snapshotName"`
	RestartLSN   string `json:"restartLSN"`
}

func CreateSlot(dsn, slot string) (*SlotInfo, error) {
	pgConfig, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pgConfig.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	res, err := pglogrepl.CreateReplicationSlot(context.Background(), conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
		Mode:      pglogrepl.LogicalReplication,
	})
	if err != nil {
		return nil, err
	}
	return &SlotInfo{
		Name:         res.SlotName,
		Plugin:       res.OutputPlugin,
		Database:     pgConfig.Database,
		SnapshotName: res.SnapshotName,
		RestartLSN:   res.ConsistentPoint,
	}, nil
}

func DropSlot(dsn, slot string) error {
	pgConfig, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return err
	}
	pgConfig.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	err = pglogrepl.DropReplicationSlot(context.Background(), conn, slot, pglogrepl.DropReplicationSlotOptions{
		Wait: true,
	})
	if err != nil {
		return err
	}
	return nil
}

type commandAndParams struct {
	SQL    string
	Params []any
}

func RevertChangeSet(dsn string, changeset []Change) error {
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	commands := make([]commandAndParams, 0, len(changeset))
	for _, change := range changeset {
		sql, params := change.RevertSQL()
		if sql == "" {
			continue
		}
		commands = append(commands, commandAndParams{
			SQL:    sql,
			Params: params,
		})
	}
	slices.Reverse(commands)
	for _, cmd := range commands {
		_, err := tx.Exec(context.Background(), cmd.SQL, cmd.Params...)
		if err != nil {
			return err
		}
	}
	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}

	return nil
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
