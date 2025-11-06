package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	_ "github.com/litesql/sqlite"
)

var (
	publication string
	indexFromFK bool
	ignoreFK    bool
	globalFK    = make(map[string][]foreignKey)
)

func main() {
	flag.StringVar(&publication, "publication", "", "Filter tables by publication name")
	flag.BoolVar(&indexFromFK, "indexFromFK", false, "Create index from foreign keys")
	flag.BoolVar(&ignoreFK, "ignoreFK", false, "Ignore foreign keys creation")
	flag.Parse()

	// pg2sqlite postgres://user:password@host:port/database?search_path=my_schema,public my_db.sqlite3
	if len(flag.Args()) != 2 {
		log.Fatal("usage: pg2sqlite [POSTGRES_URL] [SQLITE_FILEPATH]")
	}

	postgresDSN := flag.Arg(0)
	sqliteDSN := flag.Arg(1)

	pgConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	pgConn, err := pgx.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer pgConn.Close(context.Background())

	db, err := sql.Open("litesql", sqliteDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tables, err := tables(pgConn, publication)
	if err != nil {
		log.Fatal(err)
	}
	if len(tables) == 0 {
		fmt.Println("No tables found")
		return
	}

	if !ignoreFK {
		for _, t := range tables {
			fks, err := foreignKeys(pgConn, t)
			if err != nil {
				log.Fatalf("failed to load FKs: %v", err)
			}
			if len(fks) == 0 {
				continue
			}
			globalFK[t.ID()] = []foreignKey{}
			for _, fk := range fks {
				contains := slices.ContainsFunc(tables, func(st schemaTable) bool {
					return st.ID() == t.ID()
				})
				if !contains {
					slog.Warn("Ignore fk", "name", fk.name, "table", t.ID(), "references", fk.references.ID())
					continue
				}
				globalFK[t.ID()] = append(globalFK[t.ID()], fk)
			}
		}
	}

	slog.Info("Starting copy", "total_tables", len(tables))
	for _, t := range tables {
		slog.Info("Creating table", "name", t.table)
		err := createTable(pgConn, t, db)
		if err != nil {
			log.Fatal(fmt.Errorf("failed to create table %s.%s: %v", t.schema, t.table, err))
		}
		err = createIndexes(pgConn, t, db)
		if err != nil {
			log.Fatal(fmt.Errorf("failed to create table indexes for %s.%s: %v", t.schema, t.table, err))
		}
		if indexFromFK {
			err = createIndexesFromFKs(pgConn, t, db)
			if err != nil {
				log.Fatal(fmt.Errorf("failed to create table indexes from fk for %s.%s: %v", t.schema, t.table, err))
			}
		}

		slog.Info("Populating table", "name", t.table)
		err = copyData(pgConn, t, db)
		if err != nil {
			log.Fatal(fmt.Errorf("failed to copy data from table %s.%s: %v", t.schema, t.table, err))
		}
	}
	slog.Info("Database created", "path", sqliteDSN)
}

type schemaTable struct {
	schema    string
	table     string
	rowFilter string
}

func (st schemaTable) ID() string {
	return strings.ToLower(fmt.Sprintf("%s.%s", st.schema, st.table))
}

func tables(conn *pgx.Conn, publication string) ([]schemaTable, error) {
	var (
		err  error
		rows pgx.Rows
	)
	if publication != "" {
		rows, err = conn.Query(context.Background(),
			`SELECT schemaname, tablename, COALESCE(rowfilter, '') FROM pg_publication_tables WHERE pubname = $1`, publication)
	} else {
		rows, err = conn.Query(context.Background(),
			`SELECT table_schema, table_name, ''
    		FROM information_schema.tables
    		WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []schemaTable
	for rows.Next() {
		var val schemaTable
		err := rows.Scan(&val.schema, &val.table, &val.rowFilter)
		if err != nil {
			return nil, err
		}
		list = append(list, val)
	}
	return list, nil
}

func createTable(conn *pgx.Conn, st schemaTable, db *sql.DB) error {
	rows, err := conn.Query(context.Background(),
		`SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2`, st.schema, st.table)
	if err != nil {
		return err
	}

	var def []string
	for rows.Next() {
		var name, typ, nullable string
		err := rows.Scan(&name, &typ, &nullable)
		if err != nil {
			return err
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("%s %s", name, sqliteType(typ)))
		if nullable == "NO" {
			sb.WriteString(" NOT NULL")
		}
		def = append(def, sb.String())
	}

	pks, err := primaryKeys(conn, st)
	if err != nil {
		return err
	}
	if len(pks) > 0 {
		def = append(def, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pks, ", ")))
	}

	fks, ok := globalFK[st.ID()]
	if ok {
		for _, fk := range fks {
			def = append(def, fk.SQL())
		}
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS [%s](\n\t%s\n)", st.table, strings.Join(def, ",\n\t"))
	_, err = db.Exec(query)
	return err
}

func createIndexes(conn *pgx.Conn, st schemaTable, db *sql.DB) error {
	rows, err := conn.Query(context.Background(),
		`SELECT i.relname as index_name,    
    	ix.indisunique as unique,
    	ARRAY_AGG(a.attname) as column_names
		FROM
    		pg_class t,
    		pg_class i,
		    pg_index ix,
    		pg_attribute a,
			pg_namespace n
		WHERE
			t.relnamespace = n.oid
    		AND t.oid = ix.indrelid
    		AND i.oid = ix.indexrelid
    		AND a.attrelid = t.oid
    		AND a.attnum = ANY(ix.indkey)
    		AND t.relkind = 'r'
    		AND ix.indisprimary = 'f'    
    		AND n.nspname = $1
			AND t.relname = $2			   
		GROUP BY    
    		i.relname,    
    		ix.indisunique
		ORDER BY
    		i.relname;`, st.schema, st.table)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name    string
			unique  bool
			columns []string
		)
		err := rows.Scan(&name, &unique, &columns)
		if err != nil {
			return err
		}
		var uniqueStr string
		if unique {
			uniqueStr = "UNIQUE"
		}
		if len(columns) > 0 {
			query := fmt.Sprintf("CREATE %s INDEX IF NOT EXISTS [%s] ON [%s](%s)", uniqueStr, name, st.table, strings.Join(columns, ", "))
			_, err = db.Exec(query)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func createIndexesFromFKs(conn *pgx.Conn, st schemaTable, db *sql.DB) error {
	rows, err := conn.Query(context.Background(),
		`SELECT kcu.constraint_name,       
       	ARRAY_AGG(kcu.column_name) as fk_column       
		FROM information_schema.table_constraints tco
			JOIN information_schema.key_column_usage kcu
          		ON tco.constraint_schema = kcu.constraint_schema
          		AND tco.constraint_name = kcu.constraint_name
			JOIN information_schema.referential_constraints rco
          		ON tco.constraint_schema = rco.constraint_schema
          		AND tco.constraint_name = rco.constraint_name
			JOIN information_schema.key_column_usage rel_kcu
          		ON rco.unique_constraint_schema = rel_kcu.constraint_schema
          		AND rco.unique_constraint_name = rel_kcu.constraint_name
          		AND kcu.ordinal_position = rel_kcu.ordinal_position
		WHERE tco.constraint_type = 'FOREIGN KEY'
			AND kcu.table_schema = $1
			AND kcu.table_name = $2
		GROUP BY kcu.constraint_name`, st.schema, st.table)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name    string
			columns []string
		)
		err := rows.Scan(&name, &columns)
		if err != nil {
			return err
		}
		if len(columns) > 0 {
			query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS [%s] ON [%s](%s)", name, st.table, strings.Join(columns, ", "))
			_, err = db.Exec(query)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func copyData(conn *pgx.Conn, st schemaTable, db *sql.DB) error {
	query := fmt.Sprintf("SELECT * FROM %s.%s", st.schema, st.table)
	if st.rowFilter != "" {
		query += " WHERE " + st.rowFilter
	}
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var columns []string
	for _, fd := range rows.FieldDescriptions() {
		columns = append(columns, fd.Name)
	}
	stmt, err := db.Prepare(fmt.Sprintf("REPLACE INTO [%s](%s) VALUES(%s)", st.table, strings.Join(columns, ", "), placeholders(len(columns))))
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	txStmt := tx.Stmt(stmt)
	i := 0
	for rows.Next() {
		i++
		if i%10000 == 0 {
			slog.Info("Inserting records", "table", st.table, "records", i)
			err = tx.Commit()
			if err != nil {
				return err
			}
			tx, err = db.Begin()
			if err != nil {
				return err
			}
			txStmt = tx.Stmt(stmt)
		}
		values, err := rows.Values()
		if err != nil {
			return err
		}
		for i, v := range values {
			switch v.(type) {
			case map[string]any:
				jsonData, err := json.Marshal(v)
				if err != nil {
					return err
				}
				values[i] = string(jsonData)
			}
		}
		_, err = txStmt.Exec(values...)
		if err != nil {
			return err
		}
	}
	slog.Info("Inserting records", "table", st.table, "records", i)
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func primaryKeys(conn *pgx.Conn, st schemaTable) ([]string, error) {
	rows, err := conn.Query(context.Background(),
		`SELECT a.attname
			FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = $1::regclass
			AND i.indisprimary;`, fmt.Sprintf("%s.%s", st.schema, st.table))
	if err != nil {
		return nil, fmt.Errorf("failed to query %q pk columns: %w", st.table, err)
	}
	defer rows.Close()

	pk := make([]string, 0)
	for rows.Next() {
		var col string
		err := rows.Scan(&col)
		if err != nil {
			return nil, fmt.Errorf("failed to scan %q pk columns: %w", st.table, err)
		}
		pk = append(pk, col)
	}
	return pk, nil
}

type foreignKey struct {
	name              string
	table             schemaTable
	columns           []string
	references        schemaTable
	referencesColumns []string
	onUpdate          string
	onDelete          string
}

func (f foreignKey) SQL() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("FOREIGN KEY (%s) REFERENCES [%s](%s)", strings.Join(f.columns, ", "), f.references.table, strings.Join(f.referencesColumns, ", ")))
	if f.onDelete != "" {
		sb.WriteString(" ON DELETE ")
		sb.WriteString(f.onDelete)
	}
	if f.onUpdate != "" {
		sb.WriteString(" ON UPDATE ")
		sb.WriteString(f.onUpdate)
	}
	return sb.String()
}

func foreignKeys(conn *pgx.Conn, st schemaTable) ([]foreignKey, error) {
	rows, err := conn.Query(context.Background(),
		`SELECT 
  			tc.constraint_name,  
  			rc.update_rule AS on_update,
  			rc.delete_rule AS on_delete,
			ccu.constraint_schema AS references_schema,
  			ccu.table_name AS references_table,
  			ARRAY_AGG(DISTINCT kcu.column_name),
  			ARRAY_AGG(DISTINCT ccu.column_name) AS references_field
		FROM information_schema.table_constraints tc
		LEFT JOIN information_schema.key_column_usage kcu
  			ON tc.constraint_catalog = kcu.constraint_catalog
  			AND tc.constraint_schema = kcu.constraint_schema
  			AND tc.constraint_name = kcu.constraint_name
		LEFT JOIN information_schema.referential_constraints rc
  			ON tc.constraint_catalog = rc.constraint_catalog
  			AND tc.constraint_schema = rc.constraint_schema
  			AND tc.constraint_name = rc.constraint_name
		LEFT JOIN information_schema.constraint_column_usage ccu
  			ON rc.unique_constraint_catalog = ccu.constraint_catalog
  			AND rc.unique_constraint_schema = ccu.constraint_schema
  			AND rc.unique_constraint_name = ccu.constraint_name
		WHERE tc.constraint_type in ('FOREIGN KEY')
			AND tc.table_schema = $1
			AND tc.table_name = $2
		GROUP BY tc.constraint_name, rc.update_rule, rc.delete_rule, ccu.constraint_schema, ccu.table_name`, st.schema, st.table)
	if err != nil {
		return nil, fmt.Errorf("failed to query %q pk columns: %w", st.table, err)
	}
	defer rows.Close()

	fks := make([]foreignKey, 0)
	for rows.Next() {
		fk := foreignKey{
			table: st,
			references: schemaTable{
				schema: st.schema,
			},
		}
		err := rows.Scan(&fk.name, &fk.onUpdate, &fk.onDelete, &fk.references.schema, &fk.references.table, &fk.columns, &fk.referencesColumns)
		if err != nil {
			return nil, fmt.Errorf("failed to scan %q pk columns: %w", st.table, err)
		}
		fks = append(fks, fk)
	}
	return fks, nil
}

func sqliteType(pgType string) string {
	switch pgType {
	case "bytea":
		return "BLOB"
	case "smallint", "int", "integer", "bigint", "smallserial", "serial":
		return "INTEGER"
	case "real", "float4", "double precision", "float8", "numeric", "decimal", "money":
		return "REAL"
	case "json", "jsonb":
		return "JSONB"
	default:
		return "TEXT"
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
