package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

func nonMakedFiles(files []string, makedList []string) []string {
	nmFiles := files
	for i := len(files) - 1; i >= 0; i-- {
		for j := 0; j < len(makedList); j++ {
			if files[i] == makedList[j] {
				nmFiles = RemoveIndex(nmFiles, i)
			}
		}
	}
	return nmFiles
}

func fileToList(files []string, ) []string {
	var sqlList []string
	for i := 0; i < len(files); i++ {
		file, err := os.Open(files[i]) // Read files
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err = file.Close(); err != nil {
				log.Fatal(err)
			}
		}()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			sqlList = append(sqlList, scanner.Text())
		}
	}
	return sqlList
}

func migrate(dbConnectInfo string, sqlFilePath string)  {
	ctx := context.Background()
	dbpool, err := pgxpool.Connect(ctx, dbConnectInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	_, err = dbpool.Exec(ctx, "create table if not exists migrations (id serial, filename varchar)") //Creating table for already migrated
	var makedFilesName string
	var makedList []string
	var filesCont int
	err = dbpool.QueryRow(ctx, "select count(*) from migrations").Scan(&filesCont)
	for i := 0; i < filesCont; i++ {
		err = dbpool.QueryRow(ctx, "select filename from migrations where id="+strconv.Itoa(i+1)).Scan(&makedFilesName)
		makedList = append(makedList, makedFilesName)
	}

	files, err := filepath.Glob(sqlFilePath + "*.sql") // Getting list of all files in dir
	if err != nil {
		log.Fatal(err)
	}

	sort.Strings(files)

	nmFiles := nonMakedFiles(files, makedList)
	sqlList := fileToList(nmFiles)
	for i := 0; i < len(sqlList); i++ {
		_, err = dbpool.Exec(ctx, sqlList[i]) // Sql request
	}

	for i := 0; i < len(nmFiles); i++ {
		_, err = dbpool.Exec(ctx, "insert into migrations(filename) values ('"+nmFiles[i]+"')")
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Sql requests have been sent")
}