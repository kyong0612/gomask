package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"gomask/repository"
	"io/ioutil"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"

	log "github.com/sirupsen/logrus"
)

func main() {
	fmt.Printf("#### start masking ####\n\n")

	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	repo, err := repository.New(
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASS"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)
	if err != nil {
		log.Fatalln(err)
	}

	// Read json file
	bytes, err := ioutil.ReadFile("target.json")
	if err != nil {
		log.Fatal(err)
	}
	var t []DB
	if err := json.Unmarshal(bytes, &t); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	err = repo.Tx(ctx, func(repo repository.Repository) error {

		for _, db := range t {
			err = repo.Use(db.Name)
			if err != nil {
				log.Fatalln(err)
			}
			for _, table := range db.Tables {
				for _, column := range table.Columns {
					switch column.Kind {
					case "master":
						err := repo.MasterMasking(ctx, table.Name, column.Name)
						if err != nil && err != sql.ErrNoRows {
							return err
						}
					case "json":
						err := repo.JsonMasking(ctx, table.Name, column.Name)
						if err != nil && err != sql.ErrNoRows {
							return err
						}
					default:
						err := repo.DefaultMaking(ctx, table.Name, column.Name)
						if err != nil && err != sql.ErrNoRows {
							return err
						}
					}
				}

			}
		}

		return nil
	})
	if err != nil {
		log.Error("error in transaction:\n", err)
	}

	fmt.Println("\n#### finish ####")

}
