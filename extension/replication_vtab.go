package extension

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/litesql/postgresql/replication"
	"github.com/walterwanderley/sqlite"
)

type ReplicationVirtualTable struct {
	virtualTableName   string
	conn               *sqlite.Conn
	timeout            time.Duration
	subscriptions      []*replication.Subscription
	stmtMu             sync.Mutex
	loadPositionStmt   *sqlite.Stmt
	updatePositionStmt *sqlite.Stmt
	mu                 sync.Mutex
	logger             *slog.Logger
	loggerCloser       io.Closer
}

func NewReplicationVirtualTable(virtualTableName string, conn *sqlite.Conn, timeout time.Duration, positionTrackerTable string, loggerDef string) (*ReplicationVirtualTable, error) {
	logger, loggerCloser, err := loggerFromConfig(loggerDef)
	if err != nil {
		return nil, err
	}
	loadStmt, _, err := conn.Prepare("SELECT position FROM " + positionTrackerTable + " WHERE slot = ?")
	if err != nil {
		return nil, fmt.Errorf("preparing position loader statement: %w", err)
	}
	updateStmt, _, err := conn.Prepare("REPLACE INTO " + positionTrackerTable + "(slot, position) VALUES(?, ?)")
	if err != nil {
		return nil, fmt.Errorf("preparing position tracker statement: %w", err)
	}

	return &ReplicationVirtualTable{
		virtualTableName:   virtualTableName,
		conn:               conn,
		timeout:            timeout,
		loadPositionStmt:   loadStmt,
		updatePositionStmt: updateStmt,
		logger:             logger,
		loggerCloser:       loggerCloser,
		subscriptions:      make([]*replication.Subscription, 0),
	}, nil
}

func (vt *ReplicationVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{EstimatedCost: 1000000}, nil
}

func (vt *ReplicationVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return newSubscriptionsCursor(vt.subscriptions), nil
}

func (vt *ReplicationVirtualTable) Disconnect() error {
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	for _, subscription := range vt.subscriptions {
		subscription.Stop()
	}
	err = errors.Join(err, vt.loadPositionStmt.Finalize(), vt.updatePositionStmt.Finalize())
	return err
}

func (vt *ReplicationVirtualTable) Destroy() error {
	return nil
}

func (vt *ReplicationVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	if len(values) < 1 || values[0].Text() == "" {
		return 0, fmt.Errorf("dsn is required")
	}
	dsn := values[0].Text()
	if len(values) < 2 || values[1].Text() == "" {
		return 0, fmt.Errorf("slot is required")
	}
	slot := values[1].Text()
	var publication string
	if len(values) > 2 {
		publication = values[2].Text()
	}

	plugin := "pgoutput"
	if len(values) > 3 && values[3].Text() != "" {
		plugin = values[3].Text()
	}

	cfg := replication.Config{
		DSN:             dsn,
		SlotName:        slot,
		PublicationName: publication,
		Timeout:         vt.timeout,
	}

	vt.mu.Lock()
	defer vt.mu.Unlock()
	if vt.contains(slot) {
		return 0, fmt.Errorf("already using the %q slot", slot)
	}

	var (
		subscription *replication.Subscription
		err          error
	)
	switch plugin {
	case "", "pgoutput":
		subscription, err = replication.NewPgOutput(cfg, vt.handler(slot))
		if err != nil {
			return 0, err
		}
	case "wal2json":
		subscription, err = replication.NewWal2Json(cfg, vt.handler(slot))
		if err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unsupported plugin: %q", plugin)
	}
	err = subscription.Start(context.Background(), vt.logger, vt.loader(slot))
	if err != nil {
		return 0, err
	}
	vt.subscriptions = append(vt.subscriptions, subscription)

	return 1, nil
}

func (vt *ReplicationVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q are not supported", vt.virtualTableName)
}

func (vt *ReplicationVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("REPLACE operations on %q are not supported", vt.virtualTableName)
}

func (vt *ReplicationVirtualTable) Delete(v sqlite.Value) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	index := v.Int()
	// slices are 0 based
	index--

	if index >= 0 && index < len(vt.subscriptions) {
		rh := vt.subscriptions[index]
		rh.Stop()
		vt.subscriptions = slices.Delete(vt.subscriptions, index, index+1)
	}

	return nil
}

func (vt *ReplicationVirtualTable) contains(slot string) bool {
	for _, rh := range vt.subscriptions {
		if rh.SlotName() == slot {
			return true
		}
	}
	return false
}

func (vt *ReplicationVirtualTable) loader(slot string) replication.CheckpointLoader {
	return func() (pglogrepl.LSN, error) {
		err := vt.loadPositionStmt.Reset()
		if err != nil {
			return 0, err
		}
		vt.loadPositionStmt.BindText(1, slot)
		hasRow, err := vt.loadPositionStmt.Step()
		if err != nil {
			return 0, fmt.Errorf("loading last position for slot %q: %w", slot, err)
		}
		if hasRow {
			positionStr := vt.loadPositionStmt.ColumnText(0)
			position, err := pglogrepl.ParseLSN(positionStr)
			if err != nil {
				return 0, fmt.Errorf("parsing last position %q for slot %q: %w", positionStr, slot, err)
			}
			vt.logger.Info("loaded last saved position", "slot", slot, "position", position)
			return position, nil
		}
		vt.logger.Info("no saved position found, starting from beginning", "slot", slot)
		return 0, nil
	}
}

func (vt *ReplicationVirtualTable) handler(slot string) replication.HandleChanges {
	return func(changeset []replication.Change, currentPosition pglogrepl.LSN) error {
		vt.stmtMu.Lock()
		defer vt.stmtMu.Unlock()

		vt.logger.Debug("applying changeset", "slot", slot, "current_position", currentPosition)

		err := vt.conn.Exec("BEGIN", nil)
		if err != nil {
			return err
		}
		defer vt.conn.Exec("ROLLBACK", nil)

		for _, change := range changeset {
			if change.Schema == "public" {
				change.Schema = "main"
			}
			var sql string
			switch change.Kind {
			case "INSERT":
				sql = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", change.Schema, change.Table, strings.Join(change.ColumnNames, ", "), placeholders(len(change.ColumnValues)))
				err = vt.conn.Exec(sql, nil, change.ColumnValues...)
			case "UPDATE":
				setClause := make([]string, len(change.ColumnNames))
				for i, col := range change.ColumnNames {
					setClause[i] = fmt.Sprintf("%s = ?", col)
				}
				if len(change.OldKeys.KeyNames) == 0 {
					return fmt.Errorf("missing old keys for update on table %s.%s", change.Schema, change.Table)
				}
				whereClause := make([]string, len(change.OldKeys.KeyNames))
				for i, col := range change.OldKeys.KeyNames {
					whereClause[i] = fmt.Sprintf("%s = ?", col)
				}
				sql = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", change.Schema, change.Table, strings.Join(setClause, ", "), strings.Join(whereClause, " AND "))
				args := append(change.ColumnValues, change.OldKeys.KeyValues...)
				err = vt.conn.Exec(sql, nil, args...)
			case "DELETE":
				whereClause := make([]string, len(change.ColumnNames))
				for i, col := range change.ColumnNames {
					whereClause[i] = fmt.Sprintf("%s = ?", col)
				}
				sql = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", change.Schema, change.Table, strings.Join(whereClause, " AND "))
				err = vt.conn.Exec(sql, nil, change.ColumnValues...)
			default:
				continue
			}
			if err != nil {
				vt.logger.Error("failed to exec statement", "sql", sql, "error", err)
				return fmt.Errorf("failed to exec %q: %w", sql, err)
			}
		}
		err = vt.updatePositionStmt.Reset()
		if err != nil {
			vt.logger.Error("failed to reset position tracker statement", "slot", slot, "position", currentPosition, "error", err)
			return err
		}
		vt.updatePositionStmt.BindText(1, slot)
		vt.updatePositionStmt.BindText(2, currentPosition.String())
		_, err = vt.updatePositionStmt.Step()
		if err != nil {
			vt.logger.Error("failed to update position tracker", "slot", slot, "position", currentPosition, "error", err)
			return fmt.Errorf("updating position tracker: %w", err)
		}
		return vt.conn.Exec("COMMIT", nil)
	}
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	for i := range n {
		b.WriteString(fmt.Sprintf("?%d,", i+1))
	}
	return strings.TrimRight(b.String(), ",")
}

type subscriptionsCursor struct {
	data    []*replication.Subscription
	current *replication.Subscription // current row that the cursor points to
	rowid   int64                     // current rowid .. negative for EOF
}

func newSubscriptionsCursor(data []*replication.Subscription) *subscriptionsCursor {
	slices.SortFunc(data, func(a, b *replication.Subscription) int {
		return cmp.Compare(a.SlotName(), b.SlotName())
	})
	return &subscriptionsCursor{
		data: data,
	}
}

func (c *subscriptionsCursor) Next() error {
	// EOF
	if c.rowid < 0 || int(c.rowid) >= len(c.data) {
		c.rowid = -1
		return sqlite.SQLITE_OK
	}
	// slices are zero based
	c.current = c.data[c.rowid]
	c.rowid += 1

	return sqlite.SQLITE_OK
}

func (c *subscriptionsCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		ctx.ResultText(c.current.DSN())
	case 1:
		ctx.ResultText(c.current.SlotName())
	case 2:
		ctx.ResultText(c.current.PublicationName())
	case 3:
		if lastError := c.current.LastError(); lastError != nil {
			ctx.ResultText(fmt.Sprintf("%s (%s)", c.current.DecodePlugin(), lastError.Error()))
		} else {
			ctx.ResultText(c.current.DecodePlugin())
		}
	}
	return nil
}

func (c *subscriptionsCursor) Filter(int, string, ...sqlite.Value) error {
	c.rowid = 0
	return c.Next()
}

func (c *subscriptionsCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *subscriptionsCursor) Eof() bool {
	return c.rowid < 0
}

func (c *subscriptionsCursor) Close() error {
	return nil
}
