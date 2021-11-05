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

	for _, db := range t {
		err = repo.Use(db.Name)
		if err != nil {
			log.Fatalln(err)
		}
		err = repo.Tx(ctx, func(txRepo repository.Repository) error {

			for _, table := range db.Tables {
				for _, column := range table.Columns {

					switch column.Kind {
					case "default":
						err := txRepo.DefaultMaking(ctx, table.Name, column.Name)
						if err != nil && err != sql.ErrNoRows {
							return err
						}
					case "int":
						err := txRepo.IntMaking(ctx, table.Name, column.Name)
						if err != nil && err != sql.ErrNoRows {
							return err
						}
					case "master":
						err := txRepo.MasterMasking(ctx, table.Name, column.Name)
						if err != nil && err != sql.ErrNoRows {
							return err
						}
					case "json":
						err := txRepo.JsonMasking(ctx, table.Name, column.Name)
						if err != nil && err != sql.ErrNoRows {
							return err
						}
					default:
						return fmt.Errorf("[Masking kind does not match] %s", column.Kind)
					}
				}

			}
			return nil

		})

	}
	if err != nil {
		log.Error("error in transaction:\n", err)
	}

	fmt.Println("\n#### finish ####")

}
