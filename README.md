# Migrator

This tool helps to roll migration scripts to the PostgreSQL database. 
Migration tool accepts directory with SQL files and runs each of them in alphabetical order. 
If the SQL file from the directory has already been run before, it will be skipped.

## Quick start

```sh
go get -u github.com/skofli/migrator
```

```go
package main

import "github.com/skofli/migrator"

func main() {
  migrator.Migrate("postgres://username:pass@database_host:port/database", "sqlMigrationPath")
}
```
