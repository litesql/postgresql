package replication

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type handler func(pglogrepl.XLogData) error
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
	cfg       Config
	handle    handler
	lastError error
	quit      chan struct{}
}

func Subscribe(cfg Config, handler HandleChanges) (*Subscription, error) {
	if cfg.SlotName == "" {
		return nil, fmt.Errorf("slotName is required")
	}
	if cfg.PublicationName == "" {
		return nil, fmt.Errorf("publicationName is required")
	}
	cfg.pluginArgs = []string{`"proto_version" '1', "publication_names" '` + cfg.PublicationName + `'`}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &Subscription{
		cfg:    cfg,
		handle: pgOutputAdapter(handler),
		quit:   make(chan struct{}),
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

func (s *Subscription) Start(ctx context.Context, logger *slog.Logger, loadCheckpoint CheckpointLoader) error {
	cfg := s.cfg

	conn, err := pgconn.Connect(ctx, cfg.DSN)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %w", err)
	}

	currentPosition, err := loadCheckpoint()
	if err != nil {
		logger.Error("failed to load previous position", "error", err)
		sysident, err := pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			conn.Close(ctx)
			return fmt.Errorf("IdentifySystem failed: %w", err)
		}
		currentPosition = sysident.XLogPos
		logger.Info("Starting replication from current system position", "position", currentPosition)
	} else {
		logger.Info("Loaded previous position", "position", currentPosition)
	}

	err = pglogrepl.StartReplication(ctx, conn, cfg.SlotName, currentPosition,
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

	go func() {
		defer conn.Close(ctx)
		for {
			select {
			case <-s.quit:
				logger.Warn("Stopping replication handler", "slot", cfg.SlotName)
				return
			default:
				if time.Now().After(nextStandbyMessageDeadline) {
					err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: currentPosition})
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
						err = s.handle(xld)
						if err != nil {
							logger.Error("handle failed", "error", err)
							s.lastError = fmt.Errorf("handle failed: %w", err)
							return
						}
						currentPosition = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
					default:
						logger.Warn("received unexpected CopyData message", "byteID", msg.Data[0])
					}
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
