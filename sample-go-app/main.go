package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ianschenck/envflag"
)

type User struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

func main() {
	var (
		login    = envflag.String("MYSQL_USER", "root", "address where mysql db is listening")
		password = envflag.String("MYSQL_PASSWORD", "password", "address where mysql db is listening")
		host     = envflag.String("MYSQL_HOST", "mysql", "address where mysql db is listening")
		port     = envflag.String("MYSQL_PORT", "3306", "address where mysql db is listening")
		database = envflag.String("MYSQL_DATABASE", "cluster_demo", "address where mysql db is listening")
		dbDriver = "mysql"
	)
	envflag.Parse()

	// Connect to MySQL server (without specifying the database)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", *login, *password, *host, *port)
	db, err := sql.Open(dbDriver, dsn)
	if err != nil {
		log.Fatalf("connecting to MySQL: %s", err)
	}
	defer db.Close()
	log.Println("successfully connected to MySQL")

	// Create the database if it doesn't exist
	createDBQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *database)
	_, err = db.Exec(createDBQuery)
	if err != nil {
		log.Fatalf("creating database: %s", err)
	}

	// Switch to the newly created database
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", *login, *password, *host, *port, *database)
	db, err = sql.Open(dbDriver, dsn)
	if err != nil {
		log.Fatalf("connecting to MySQL database: %s", err)
	}
	defer db.Close()

	// Create the users table if it doesn't exist
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS users (
		uuid VARCHAR(36) PRIMARY KEY,
		name VARCHAR(100) NOT NULL
	);`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatalf("creating table: %s", err)
	}

	// Insert some initial user data if the table is empty
	insertInitialUsersQuery := `
	INSERT INTO users (uuid, name)
		VALUES 
			('123e4567-e89b-12d3-a456-426614174000', 'Alice'),
			('123e4567-e89b-12d3-a456-426614174001', 'Bob'),
			('123e4567-e89b-12d3-a456-426614174002', 'Charlie')
		ON DUPLICATE KEY UPDATE uuid=uuid;`
	_, err = db.Exec(insertInitialUsersQuery)
	if err != nil {
		log.Fatalf("inserting initial users: %s", err)
	}

	log.Println("Database and table setup complete, with initial users inserted")

	uh := userHandler{
		ctx: context.Background(),
		db:  db,
	}

	// Apply CORS middleware to the user handler
	http.Handle("/users", corsMiddleware(uh))
	http.ListenAndServe(":8080", nil)
}

type userHandler struct {
	ctx context.Context
	db  *sql.DB
}

func (uh userHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		uh.getUsers(w, r)
	case "POST":
		uh.createUser(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

var (
	insertUserQuery = "INSERT INTO users (uuid, name) VALUES (?, ?);"
	getUsersQuery   = "SELECT uuid, name from users;"
)

func (uh userHandler) createUser(w http.ResponseWriter, r *http.Request) {
	var u User
	err := json.NewDecoder(r.Body).Decode(&u)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	stmt, err := uh.db.PrepareContext(uh.ctx, insertUserQuery)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer stmt.Close()
	_, err = stmt.ExecContext(uh.ctx, &u.UUID, &u.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("user added"))
}

func (uh userHandler) getUsers(w http.ResponseWriter, r *http.Request) {
	rows, err := uh.db.QueryContext(uh.ctx, getUsersQuery)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.UUID, &u.Name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res, err := json.Marshal(users)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

// corsMiddleware adds CORS headers to the response
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
