package extension

import (
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/postgresql/config"
)

func registerFunc(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
	if err := api.CreateModule(config.DefaultReplicationVTabName, &ReplicationModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("pg_drop_slot", &DropSlot{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("pg_info", &Info{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("pg_system", &Identify{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}

	return sqlite.SQLITE_OK, nil
}
