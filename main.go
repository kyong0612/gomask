package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"gomask/repository"
	"io/ioutil"
	"os"
	"time"

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
		err = repo.Use(ctx, db.Name)
		if err != nil {
			log.Fatalln(err)
		}
		for _, table := range db.Tables {
			for _, column := range table.Columns {
				// // wait
				// setTimeout(1)
				switch column.Kind {
				case "default":
					err = repo.DefaultMaking(ctx, table.Name, column.Name)
				case "int":
					err = repo.IntMaking(ctx, table.Name, column.Name)
				case "master":
					err = repo.MasterMasking(ctx, table.Name, column.Name)
				case "json":
					err = repo.JsonMasking(ctx, table.Name, column.Name)
				case "topOne":
					err = repo.TopOneMaking(ctx, table.Name, column.Name)

				case "threeNineAdd":
					err = repo.ThreeNineAddMaking(ctx, table.Name, column.Name)
				default:
					err = fmt.Errorf("[Masking kind does not match] %s", column.Kind)
				}

				if err != nil && err != sql.ErrNoRows {
					log.Errorf("error in exec:\n%s\n", err)
				}
			}

		}

	}

	fmt.Println("\n#### finish ####")

}

func setTimeout(second int) {
	fmt.Println("waiting...")
	time.Sleep(time.Duration(second) * time.Second)
}
