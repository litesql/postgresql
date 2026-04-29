package extension

import (
	"encoding/json"

	"github.com/litesql/postgresql/replication"
	"github.com/walterwanderley/sqlite"
)

type Identify struct {
}

func (m *Identify) Args() int {
	return 1
}

func (m *Identify) Deterministic() bool {
	return false
}

func (m *Identify) Apply(ctx *sqlite.Context, values ...sqlite.Value) {
	dsn := values[0].Text()
	info, err := replication.Identify(dsn)
	if err != nil {
		ctx.ResultError(err)
		return
	}
	jsonInfo, err := json.Marshal(info)
	if err != nil {
		ctx.ResultError(err)
		return
	}
	ctx.ResultText(string(jsonInfo))
}
