package main

import (
	stdSQL "database/sql"
	"fmt"
	"os"
	"time"

	driver "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func main() {
	for i := 0; i < 10; i++ {
		err := tryConnecting()
		if err == nil {
			os.Exit(0)
		}

		time.Sleep(1 * time.Second)
	}

	fmt.Println("Failed to connect")
	os.Exit(1)
}

func tryConnecting() error {
	err := connectToMySQL()
	if err != nil {
		return err
	}

	err = connectToPostgreSQL()
	if err != nil {
		return err
	}

	return nil
}

func connectToMySQL() error {
	addr := os.Getenv("WATERMILL_TEST_MYSQL_HOST")
	if addr == "" {
		addr = "localhost"
	}
	conf := driver.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = addr

	conf.DBName = "watermill"

	db, err := stdSQL.Open("mysql", conf.FormatDSN())
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	return nil
}

func connectToPostgreSQL() error {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	db, err := stdSQL.Open("postgres", connStr)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	return nil
}
