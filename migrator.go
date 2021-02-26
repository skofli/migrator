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

type sqlReq []struct {
	filename string
	sqlList  string
}

func removeIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

func nonMakedFiles(files []string, makedList []string, sqlFilePath string) []string {
	nmFiles := files
	for i := len(files) - 1; i >= 0; i-- {
		for j := 0; j < len(makedList); j++ {
			if files[i][len(sqlFilePath):] == makedList[j] {
				nmFiles = removeIndex(nmFiles, i)
			}
		}
	}
	return nmFiles
}

func fileToList(files []string, sqlFilePath string) sqlReq {
	var sqlList sqlReq
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
		sqlList = append(sqlList, struct {
			filename string
			sqlList  string
		}{filename: files[i][len(sqlFilePath):]})
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			sqlList[i].sqlList += scanner.Text()
		}
	}
	return sqlList
}

func Migrate(dbConnectInfo string, sqlFilePath string) error{
	ctx := context.Background()
	dbpool, err := pgxpool.Connect(ctx, dbConnectInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		return err
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
	fmt.Println(files)

	sort.Strings(files)

	nmFiles := nonMakedFiles(files, makedList, sqlFilePath)
	sqlList := fileToList(nmFiles, sqlFilePath)
	for j := 0; j < len(sqlList); j++ {
		tx, err := dbpool.Begin(ctx)

		_, err = tx.Exec(ctx, sqlList[j].sqlList) // Sql request
		if err != nil {
			fmt.Println("ERROR: File {" + files[j] + "} has invalid syntax. Rollback. Files before it have been maked.")
			err = tx.Rollback(ctx)
			return err
		}
		_, err = dbpool.Exec(ctx, "insert into migrations(filename) values ('"+sqlList[j].filename+"');")
		err = tx.Commit(ctx)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return err
	}
	if len(sqlList) == 0 {
		fmt.Println("There is no one outstanding file")
	} else {
		fmt.Println("Sql requests have been sent")
	}
	return err
}

func main() {
	Migrate("postgres://skofli:@localhost:5432/test2","/home/skofli/go/src/migrator/")
}