package replication

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
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

type HandleChanges func(changeset []Change, currentPosition pglogrepl.LSN) error
type CheckpointLoader func() (pglogrepl.LSN, error)

type Config struct {
	DSN             string
	PublicationName string
	SlotName        string
	Timeout         time.Duration
	pluginArgs      []string
}

type Subscription struct {
	cfg             Config
	handleFn        HandleChanges
	lastError       error
	useNamespace    bool
	currentPosition pglogrepl.LSN
	changes         []Change
	relations       map[uint32]*pglogrepl.RelationMessageV2
	quit            chan struct{}
}

func Subscribe(cfg Config, handler HandleChanges, useNamespace bool) (*Subscription, error) {
	if cfg.SlotName == "" {
		return nil, fmt.Errorf("slotName is required")
	}
	if cfg.PublicationName == "" {
		return nil, fmt.Errorf("publicationName is required")
	}
	cfg.pluginArgs = []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", cfg.PublicationName),
		"messages 'true'",
		"streaming 'true'",
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &Subscription{
		cfg:          cfg,
		handleFn:     handler,
		useNamespace: useNamespace,
		changes:      make([]Change, 0),
		relations:    make(map[uint32]*pglogrepl.RelationMessageV2),
		quit:         make(chan struct{}),
	}, nil
}

func (s *Subscription) SlotName() string {
	return s.cfg.SlotName
}

func (s *Subscription) PublicationName() string {
	return s.cfg.PublicationName
}

func (s *Subscription) DSN() string {
	return s.cfg.DSN
}

var typeMap = pgtype.NewMap()

func (s *Subscription) Start(ctx context.Context, logger *slog.Logger, loadCheckpoint CheckpointLoader) error {
	cfg := s.cfg

	conn, err := pgconn.Connect(ctx, cfg.DSN)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %w", err)
	}

	s.currentPosition, err = loadCheckpoint()
	if err != nil {
		logger.Error("failed to load previous position", "error", err)
		sysident, err := pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			conn.Close(ctx)
			return fmt.Errorf("IdentifySystem failed: %w", err)
		}
		s.currentPosition = sysident.XLogPos
		logger.Info("Starting replication from current system position", "position", s.currentPosition)
	} else {
		logger.Info("Loaded previous position", "position", s.currentPosition)
	}

	err = pglogrepl.StartReplication(ctx, conn, cfg.SlotName, s.currentPosition,
		pglogrepl.StartReplicationOptions{
			PluginArgs: cfg.pluginArgs,
		})
	if err != nil {
		conn.Close(ctx)
		return fmt.Errorf("startReplication failed: %w", err)
	}
	logger.Info("Logical replication started", "slot", cfg.SlotName)
	standbyMessageTimeout := cfg.Timeout
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	inStream := false
	go func() {
		defer conn.Close(ctx)
		for {
			select {
			case <-s.quit:
				logger.Warn("Stopping replication handler", "slot", cfg.SlotName)
				return
			default:
				if time.Now().After(nextStandbyMessageDeadline) {
					err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: s.currentPosition})
					if err != nil {
						logger.Error("sendStandbyStatusUpdate failed", "error", err)
						s.lastError = fmt.Errorf("sendStandbyStatusUpdate failed: %w", err)
						return
					}
					nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
				}

				ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
				msg, err := conn.ReceiveMessage(ctx)
				cancel()
				if err != nil {
					if pgconn.Timeout(err) {
						continue
					}
					logger.Error("receiveMessage failed", "error", err)
					s.lastError = fmt.Errorf("receiveMessage failed: %w", err)
					return
				}

				switch msg := msg.(type) {
				case *pgproto3.CopyData:
					switch msg.Data[0] {
					case pglogrepl.PrimaryKeepaliveMessageByteID:
						pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
						if err != nil {
							logger.Error("ParsePrimaryKeepaliveMessage failed", "error", err)
							s.lastError = fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
							return
						}

						if pkm.ReplyRequested {
							nextStandbyMessageDeadline = time.Time{}
						}
					case pglogrepl.XLogDataByteID:
						xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
						if err != nil {
							logger.Error("parseXLogData failed", "error", err)
							s.lastError = fmt.Errorf("parseXLogData failed: %w", err)
							return
						}
						err = s.process(xld.WALStart, xld.WALData, xld.ServerTime, typeMap, &inStream, logger)
						if err != nil {
							logger.Error("handle failed", "error", err)
							s.lastError = fmt.Errorf("handle failed: %w", err)
							return
						}
						s.currentPosition = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
					default:
						logger.Warn("received unexpected CopyData message", "byteID", msg.Data[0])
					}
				case *pgproto3.ErrorResponse:
					logger.Error("received Postgres WAL error:" + msg.Message)
					s.lastError = fmt.Errorf("received Postgres WAL error: %v", msg.Message)
					return
				default:
					typ := fmt.Sprintf("%T", msg)
					logger.Warn("received unexpected message", "type", typ, "message", msg)
				}
			}
		}
	}()
	return nil
}

func (s *Subscription) Stop() {
	close(s.quit)
}

func (s *Subscription) LastError() error {
	return s.lastError
}

func (s *Subscription) process(walStart pglogrepl.LSN, walData []byte, serverTime time.Time, typeMap *pgtype.Map, inStream *bool, logger *slog.Logger) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		return fmt.Errorf("parse logical replication message: %w", err)
	}
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		if _, ok := s.relations[logicalMsg.RelationID]; !ok {
			change, err := s.decodeRelationChange(logicalMsg, typeMap)
			if err != nil {
				return err
			}
			s.changes = append(s.changes, change)
		}
		s.relations[logicalMsg.RelationID] = logicalMsg
	case *pglogrepl.BeginMessage:
		s.changes = make([]Change, 0)
	case *pglogrepl.CommitMessage:
		if s.handleFn != nil {
			s.currentPosition = walStart + pglogrepl.LSN(len(walData))
			return s.handleFn(s.changes, s.currentPosition)
		}
	case *pglogrepl.InsertMessageV2:
		change, err := s.decodeChange(logicalMsg.RelationID, logicalMsg.Tuple, nil)
		if err != nil {
			return err
		}
		change.Kind = "INSERT"
		change.ServerTime = serverTime
		s.changes = append(s.changes, change)
	case *pglogrepl.UpdateMessageV2:
		change, err := s.decodeChange(logicalMsg.RelationID, logicalMsg.NewTuple, logicalMsg.OldTuple)
		if err != nil {
			return err
		}
		change.Kind = "UPDATE"
		change.ServerTime = serverTime
		s.changes = append(s.changes, change)
	case *pglogrepl.DeleteMessageV2:
		change, err := s.decodeChange(logicalMsg.RelationID, logicalMsg.OldTuple, nil)
		if err != nil {
			return err
		}
		change.Kind = "DELETE"
		change.ServerTime = serverTime
		s.changes = append(s.changes, change)
	case *pglogrepl.TruncateMessageV2:
	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:
	case *pglogrepl.LogicalDecodingMessageV2:
	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
	case *pglogrepl.StreamCommitMessageV2:
	case *pglogrepl.StreamAbortMessageV2:
	default:
		typ := fmt.Sprintf("%T", logicalMsg)
		logger.Warn("Unknown message type in pgoutput stream", "type", typ)
	}
	return nil
}

func (s *Subscription) decodeRelationChange(rel *pglogrepl.RelationMessageV2, typeMap *pgtype.Map) (Change, error) {
	colNameAndType := make([]string, 0)
	for _, col := range rel.Columns {
		sqliteType := "TEXT"
		pgType, ok := typeMap.TypeForOID(col.DataType)
		if ok {
			switch pgType.Name {
			case "bytea", "varbit":
				sqliteType = "BLOB"
			case "numeric", "float4", "float8":
				sqliteType = "REAL"
			case "int2", "int4", "int8":
				sqliteType = "INTEGER"
			case "json", "jsonb":
				sqliteType = "JSONB"
			}
			colNameAndType = append(colNameAndType, fmt.Sprintf("%s %s", col.Name, sqliteType))
		}
	}
	c := Change{
		Schema: rel.Namespace,
		Table:  rel.RelationName,
		Kind:   "SQL",
	}
	tableName := c.Table
	if s.useNamespace {
		if c.Schema == "public" {
			c.Schema = "main"
		}
		tableName = fmt.Sprintf("%s.%s", c.Schema, c.Table)
	}
	c.SQL = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(%s)", tableName, strings.Join(colNameAndType, ", "))
	return c, nil
}

func (s *Subscription) decodeChange(relationID uint32, tuple *pglogrepl.TupleData, oldTuple *pglogrepl.TupleData) (Change, error) {
	rel, ok := s.relations[relationID]
	if !ok {
		return Change{}, fmt.Errorf("unknown relation ID %d", relationID)
	}
	c := Change{
		Schema: rel.Namespace,
		Table:  rel.RelationName,
	}
	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			c.ColumnNames = append(c.ColumnNames, colName)
			c.ColumnValues = append(c.ColumnValues, nil)
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return c, fmt.Errorf("error decoding column data: %w", err)
			}
			c.ColumnNames = append(c.ColumnNames, colName)
			c.ColumnValues = append(c.ColumnValues, val)
		}
	}

	if oldTuple != nil {
		for idx, col := range oldTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				c.OldKeys.KeyNames = append(c.OldKeys.KeyNames, colName)
				c.OldKeys.KeyValues = append(c.OldKeys.KeyValues, nil)
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return c, fmt.Errorf("error decoding column data: %w", err)
				}
				c.OldKeys.KeyNames = append(c.OldKeys.KeyNames, colName)
				c.OldKeys.KeyValues = append(c.OldKeys.KeyValues, val)
			}
		}
	}
	return c, nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
