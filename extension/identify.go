package extension

import (
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
	ctx.ResultText(replication.Identify(dsn))
}
