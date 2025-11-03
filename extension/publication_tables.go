package extension

import (
	"github.com/litesql/postgresql/replication"
	"github.com/walterwanderley/sqlite"
)

type PublicationTables struct {
}

func (m *PublicationTables) Args() int {
	return 1
}

func (m *PublicationTables) Deterministic() bool {
	return false
}

func (m *PublicationTables) Apply(ctx *sqlite.Context, values ...sqlite.Value) {
	dsn := values[0].Text()
	ctx.ResultText(replication.PublicationTables(dsn))
}
