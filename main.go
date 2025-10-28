package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"
	"github.com/joho/godotenv" // Library for loading .env files
)

type DataRow struct {
	FsNo            string
	SaleType        string
	AttachmentNo    string
	Customer        string
	Region          string
	Date            time.Time 
	Code            string
	Name            string
	MeasurementUnit string
	UnitPrice       float64
	SoldQuantity    float64
	NetPay          float64
}


const (
	sourceTableName = "Sales"   // MSSQL Source Table
	targetTableName = "SalesDB" // PostgreSQL Target Table
)

func main() {
	log.Println("Starting Go ETL Pipeline...")

	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Error loading .env file: %v", err)
	}

	mssqlDSN := os.Getenv("MSSQL_CONN")
	postgresDSN := os.Getenv("POSTGRES_CONN")

	if mssqlDSN == "" || postgresDSN == "" {
		log.Fatal("MSSQL_CONN and POSTGRES_CONN environment variables must be set. Check your .env file.")
	}

	sourceDB, err := sql.Open("sqlserver", mssqlDSN)
	if err != nil {
		log.Fatalf("Error connecting to MSSQL Source: %v", err)
	}
	defer sourceDB.Close()
	if err = sourceDB.Ping(); err != nil {
		log.Fatalf("Error pinging MSSQL Source: %v", err)
	}
	log.Println("Successfully connected to MSSQL Source.")

	targetDB, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatalf("Error connecting to PostgreSQL Target: %v", err)
	}
	defer targetDB.Close()
	if err = targetDB.Ping(); err != nil {
		log.Fatalf("Error pinging PostgreSQL Target: %v", err)
	}
	log.Println("Successfully connected to PostgreSQL Target.")

	if err := ensureTargetTable(targetDB); err != nil {
		log.Fatalf("Failed to prepare target table: %v", err)
	}

	log.Printf("Starting ETL from %s to %s...", sourceTableName, targetTableName)
	startTime := time.Now()

	count, err := runETL(sourceDB, targetDB)
	if err != nil {
		log.Fatalf("ETL Process failed: %v", err)
	}

	duration := time.Since(startTime)
	log.Printf("ETL Process successful! Migrated %d rows in %v.", count, duration)
}

func ensureTargetTable(db *sql.DB) error {
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			fsno VARCHAR(50) PRIMARY KEY,
			salestype VARCHAR(50),
			attachmentno VARCHAR(50),
			customer VARCHAR(100),
			region VARCHAR(50),
			sale_date DATE,
			code VARCHAR(50),
			item_name VARCHAR(100),
			measurement_unit VARCHAR(50),
			unit_price NUMERIC(12, 2),
			sold_quantity NUMERIC(12, 2),
			net_pay NUMERIC(12, 2)
		);
	`, targetTableName)

	if _, err := db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create target table: %w", err)
	}
	log.Printf("Target table '%s' is ready (fsno is PRIMARY KEY).", targetTableName)

	return nil
}

func runETL(sourceDB *sql.DB, targetDB *sql.DB) (int, error) {
	query := fmt.Sprintf(`
		SELECT fsno, salestype, attachmentno, customer, region, date, code, name, measurementunit, unitprice, soldquantity, netpay
		FROM %s ORDER BY fsno`, sourceTableName)
	rows, err := sourceDB.Query(query)
	if err != nil {
		return 0, fmt.Errorf("failed to query source data: %w", err)
	}
	defer rows.Close()

	tx, err := targetDB.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to start target transaction: %w", err)
	}
	defer tx.Rollback() 

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (fsno, salestype, attachmentno, customer, region, sale_date, code, item_name, measurement_unit, unit_price, sold_quantity, net_pay)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (fsno) DO NOTHING`, targetTableName) 

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	totalRows := 0
	log.Println("Starting data transfer...")

	for rows.Next() {
		var fsno, salestype, attachmentno, customer, region, code, name, measurementunit sql.NullString
		var date sql.NullTime
		var unitprice, soldquantity, netpay sql.NullFloat64

		if err := rows.Scan(
			&fsno, &salestype, &attachmentno, &customer, &region, &date,
			&code, &name, &measurementunit, &unitprice, &soldquantity, &netpay,
		); err != nil {
			log.Printf("Error scanning source row (count %d): %v. Skipping row.", totalRows+1, err)
			continue 
		}

		if _, err := stmt.Exec(
			fsno, salestype, attachmentno, customer, region, date,
			code, name, measurementunit, unitprice, soldquantity, netpay,
		); err != nil {
			log.Printf("Failed to insert row with fsno %s: %v", fsno.String, err)
			return totalRows, fmt.Errorf("error executing insert statement: %w", err)
		}
		totalRows++
	}

	if err := rows.Err(); err != nil {
		return totalRows, fmt.Errorf("error iterating over source rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return totalRows, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return totalRows, nil
}
