package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/guregu/sqlx"
	log "github.com/sirupsen/logrus"
)

type sqlxDB interface {
	sqlx.Ext
	sqlx.ExtContext
	sqlx.Preparer
	sqlx.PreparerContext
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	Rebind(query string) string
}

type Repository struct {
	db   sqlxDB
	root *sqlx.DB
	dataSource
}

type dataSource struct {
	user     string
	password string
	host     string
	port     string
	name     string
}

func New(user, password, host, port, name string) (Repository, error) {
	ds := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?parseTime=true&columnsWithAlias=true&loc=%s",
		user,
		password,
		fmt.Sprintf("%s:%s", host, port),
		name,
		"Asia%2FTokyo",
	)
	log.Info("[Connect] ", ds)
	db, err := sqlx.Connect("mysql", ds)
	repo := Repository{
		db:   db,
		root: db,
		dataSource: dataSource{
			user:     user,
			password: password,
			host:     host,
			port:     port,
			name:     name,
		},
	}

	return repo, err
}

func (repo *Repository) Use(database string) error {
	ds := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s",
		repo.user,
		repo.password,
		fmt.Sprintf("%s:%s", repo.host, repo.port),
		database,
	)
	log.Println("[Connect]:", ds)
	db, err := sqlx.Connect("mysql", ds)

	repo.db = db
	repo.root = db
	repo.name = database

	return err
}

const (
	UpdateDefaultMasking = `UPDATE %s SET %s = CONCAT(LEFT( %s, 1),REPEAT('*',CHAR_LENGTH(%s) - 1));`

	UpdateMasterMasking = `UPDATE %s SET %s = CONCAT(REPEAT('*', CHAR_LENGTH(%s)- TRUNCATE(CHAR_LENGTH(%s)/ 2, 0)), RIGHT(%s, TRUNCATE(CHAR_LENGTH(%s)/ 2, 0)));`
)

type Masked struct {
	Table  string `db:"table"`
	Column string `db:"column"`
}

// Leave one letter and mask
func (repo *Repository) DefaultMaking(ctx context.Context, table, column string) error {
	// for check exec sql
	q := fmt.Sprintf(UpdateDefaultMasking, table, column, column, column)
	log.Println("[SQL] " + q)

	_, err := repo.db.ExecContext(ctx, q)
	return err
}

// Leave the half number of word and mask
func (repo *Repository) MasterMasking(ctx context.Context, table, column string) error {
	// for check exec sql
	q := fmt.Sprintf(UpdateMasterMasking, table, column, column, column, column, column)
	log.Println("[SQL] " + q)

	_, err := repo.db.ExecContext(ctx, q)

	return err
}

func (repo *Repository) Tx(ctx context.Context, do func(Repository) error) error {
	tx, err := repo.root.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	child := &Repository{
		db:   tx,
		root: repo.root,
	}
	if err := do(*child); err != nil {
		if innerErr := tx.Rollback(); innerErr != nil {
			return fmt.Errorf("tx: rollback error: %w (outer error: %v)", innerErr, err)
		}
		return err
	}
	return tx.Commit()
}
