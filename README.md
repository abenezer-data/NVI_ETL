# MSSQL to Postgres ETL (In Go!)

During my internship, I needed to connect a BI tool (Metabase) to the sales data for Sales Manager. The Database admin vetoed direct access to the production MSSQL server due to security concerns.

The Solution: Build an isolated data sandbox. This custom Go pipeline securely pulls only the required Sales data from MSSQL and loads it into a dedicated PostgreSQL replica (SalesDB). Metabase only touches the replica, and the production environment stays safe. 


## 🛠️ Setup & How to Run

Prerequisites

Go 1.18+ installed.

Connection details for source (MSSQL) and target (PostgreSQL).

1. Grab Dependencies

go mod init your/repo/name/nvi_etl
go get [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb) [github.com/lib/pq](https://github.com/lib/pq) [github.com/joho/godotenv](https://github.com/joho/godotenv)


2. Configure Secrets

Create a .env file and add your database connection strings.

3. Run

The application will automatically create the SalesDB table and start the migration process.

go run main.go
