package extension

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/walterwanderley/sqlite"

	"github.com/litesql/postgresql/config"
)

type SubscriptionModule struct {
}

func (m *SubscriptionModule) Connect(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	virtualTableName := args[2]
	if virtualTableName == "" {
		virtualTableName = config.DefaultSubscriptionVTabName
	}

	var (
		useNamespace         bool
		positionTrackerTable string
		timeout              time.Duration

		logger string
		err    error
	)
	if len(args) > 3 {
		for _, opt := range args[3:] {
			k, v, ok := strings.Cut(opt, "=")
			if !ok {
				return nil, fmt.Errorf("invalid option: %q", opt)
			}
			k = strings.TrimSpace(k)
			v = sanitizeOptionValue(v)

			switch strings.ToLower(k) {
			case config.UseNamespace:
				b, err := strconv.ParseBool(v)
				if err != nil {
					return nil, fmt.Errorf("invalid %q option: %w", k, err)
				}
				useNamespace = b
			case config.PositionTrackerTable:
				positionTrackerTable = v
			case config.Timeout:
				i, err := strconv.Atoi(v)
				if err != nil {
					return nil, fmt.Errorf("invalid %q option: %w", k, err)
				}
				timeout = time.Duration(i) * time.Millisecond
			case config.Logger:
				logger = v
			}
		}
	}

	if positionTrackerTable == "" {
		positionTrackerTable = config.DefaultPositionTrackerTabName
	}

	err = conn.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s(
	slot TEXT PRIMARY KEY,
	position TEXT,
	server_time TEXT
)`, positionTrackerTable), nil)
	if err != nil {
		return nil, fmt.Errorf("creating %q table: %w", positionTrackerTable, err)
	}

	vtab, err := NewSubscriptionVirtualTable(virtualTableName, conn, timeout, positionTrackerTable, useNamespace, logger)
	if err != nil {
		return nil, err
	}
	return vtab, declare("CREATE TABLE x(connect TEXT, slot TEXT, publication TEXT)")
}

func sanitizeOptionValue(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "'")
	v = strings.TrimSuffix(v, "'")
	v = strings.TrimPrefix(v, "\"")
	v = strings.TrimSuffix(v, "\"")
	return os.ExpandEnv(v)
}
