package extension

import (
	"github.com/litesql/postgresql/replication"
	"github.com/walterwanderley/sqlite"
)

type DropSlot struct {
}

func (m *DropSlot) Args() int {
	return 2
}

func (m *DropSlot) Deterministic() bool {
	return true
}

func (m *DropSlot) Apply(ctx *sqlite.Context, values ...sqlite.Value) {
	dsn := values[0].Text()
	slot := values[1].Text()
	ctx.ResultText(replication.DropSlot(dsn, slot))
}
