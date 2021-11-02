package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
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
	log.Info("[Connect]\n", ds)
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
	// ds := fmt.Sprintf(
	// 	"%s:%s@tcp(%s)/%s?parseTime=true&columnsWithAlias=true&loc=%s",
	// 	repo.user,
	// 	repo.password,
	// 	fmt.Sprintf("%s:%s", repo.host, repo.port),
	// 	database,
	// 	"Asia%2FTokyo",
	// )
	ds := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s",
		repo.user,
		repo.password,
		fmt.Sprintf("%s:%s", repo.host, repo.port),
		database,
	)
	log.Info("[Connect]:", ds)
	db, err := sqlx.Connect("mysql", ds)

	repo.db = db
	repo.name = database

	return err
}

const (
	UpdateDefaultMasking = `
		UPDATE 
			:table
		SET 
			:column = CONCAT(
    LEFT(
      :column, 1
      )
    ), 
    REPEAT(
      '*', 
      CHAR_LENGTH(:column) - 1
    )
  );
	`

	UpdateMasterMasking = `
		UPDATE 
			:table 
		SET 
			:column = CONCAT(
    REPEAT(
      '*', 
      CHAR_LENGTH(:Column)- TRUNCATE(
        CHAR_LENGTH(:column)/ 2, 
        0
      )
    ), 
    RIGHT(
      :column, 
      TRUNCATE(
        CHAR_LENGTH(:column)/ 2, 
        0
      )
    )
  );
	
	`
)

type Masked struct {
	Table  string `db:"table"`
	Column string `db:"column"`
}

// Leave one letter and mask
func (repo *Repository) DefaultMaking(ctx context.Context, table, column string) error {
	m := Masked{
		Table:  table,
		Column: column,
	}
	_, err := repo.db.NamedExecContext(ctx, UpdateDefaultMasking, m)
	return err
}

// Leave the half number of word and mask
func (repo *Repository) MasterMasking(ctx context.Context, table, column string) error {
	m := Masked{
		Table:  table,
		Column: column,
	}
	_, err := repo.db.NamedExecContext(ctx, UpdateMasterMasking, m)

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
