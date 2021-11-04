package main

import (
	"context"
	"database/sql"
	"fmt"
	"gomask/repository"
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

	ctx := context.Background()
	err = repo.Tx(ctx, func(repo repository.Repository) error {
		err := repo.DefaultMaking(ctx, "projects", "cp_code")
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		return err
	})

	if err != nil {
		log.Error("error in transaction:\n", err)
	}

	fmt.Println("\n#### finish ####")

}
