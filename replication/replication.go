package replication

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func Identify(dsn string) string {
	conn, err := pgconn.Connect(context.Background(), dsn)
	if err != nil {
		return fmt.Sprintf(`{"error": %s}`, err.Error())
	}
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		return fmt.Sprintf(`{"error": %s}`, err.Error())
	}
	return fmt.Sprintf(`{"systemid": "%s", "timeline": %d, "xlogpos": "%s", "dbname": "%s"}`, sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)
}

func CreateSlot(dsn, slot string) string {
	conn, err := pgconn.Connect(context.Background(), dsn)
	if err != nil {
		return fmt.Sprintf(`{"error": %s}`, err.Error())
	}
	defer conn.Close(context.Background())

	res, err := pglogrepl.CreateReplicationSlot(context.Background(), conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
		Mode:      pglogrepl.LogicalReplication,
	})
	if err != nil {
		return fmt.Sprintf(`{"error": %s}`, err.Error())
	}
	return fmt.Sprintf(`{"slot": "%s", "plugin": "%s", "snapshot": "%s", "consistentPoint": "%s"}`, res.SlotName, res.OutputPlugin, res.SnapshotName, res.ConsistentPoint)
}

func DropSlot(dsn, slot string) string {
	conn, err := pgconn.Connect(context.Background(), dsn)
	if err != nil {
		return fmt.Sprintf(`{"error": %s}`, err.Error())
	}
	defer conn.Close(context.Background())

	err = pglogrepl.DropReplicationSlot(context.Background(), conn, slot, pglogrepl.DropReplicationSlotOptions{
		Wait: true,
	})
	if err != nil {
		return fmt.Sprintf(`{"error": %s}`, err.Error())
	}
	return fmt.Sprintf(`{"status": "slot %s dropped successfully"}`, slot)
}
