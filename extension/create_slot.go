package extension

import (
	"github.com/litesql/postgresql/replication"
	"github.com/walterwanderley/sqlite"
)

type CreateSlot struct {
}

func (m *CreateSlot) Args() int {
	return 2
}

func (m *CreateSlot) Deterministic() bool {
	return false
}

func (m *CreateSlot) Apply(ctx *sqlite.Context, values ...sqlite.Value) {
	dsn := values[0].Text()
	slot := values[1].Text()
	ctx.ResultText(replication.CreateSlot(dsn, slot))
}
