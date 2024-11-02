package post05_msds

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// Connection details
var (
	Hostname = ""
	Port     = 2345
	Username = ""
	Password = ""
	Database = ""
)

// Userdata is for holding full user data
// Userdata table + Username
//type Userdata struct {
//	ID          int
//	Username    string
//	Name        string
//	Surname     string
//	Description string
//}

type MSDSCourse struct {
	CID     string `json:"course_id"`
	CNAME   string `json:"course_name"`
	CPREREQ string `json:"prerequisite"`
}

func openConnection() (*sql.DB, error) {
	// connection string
	conn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		Hostname, Port, Username, Password, Database)

	// open database
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// The function returns the User ID of the username
// -1 if the user does not exist
func exists(CID string) string {
	CID = strings.ToLower(CID)

	db, err := openConnection()
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer db.Close()

	statement := fmt.Sprintf(`SELECT "CID" FROM "MSDS" where CID = '%s'`, CID)
	rows, err := db.Query(statement)

	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			fmt.Println("Scan", err)
			return ""
		}
		CID = id
	}
	defer rows.Close()
	return CID
}

// AddUser adds a new user to the database
// Returns new User ID
// -1 if there was an error
func AddUser(d MSDSCourse) string {
	d.CID = strings.ToLower(d.CID)

	db, err := openConnection()
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer db.Close()

	CID := exists(d.CID)
	if CID != "" {
		fmt.Println("Course already exists:", CID)
		return ""
	}

	insertStatement := `INSERT INTO "MSDS" ("CID", "CNAME", "CPREREQ") VALUES ($1, $2, $3)`
	_, err = db.Exec(insertStatement, d.CID, d.CNAME, d.CPREREQ)
	if err != nil {
	    fmt.Println("db.Exec()", err)
	    return ""
	}

	return CID
}

// DeleteUser deletes an existing user
func DeleteUser(CID string) error {
	db, err := openConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	// Does the ID exist?
	statement := fmt.Sprintf(`SELECT "CID" FROM "MSDS" where CID = %s`, CID)
	rows, err := db.Query(statement)

	var cid string
	for rows.Next() {
		err = rows.Scan(&cid)
		if err != nil {
			return err
		}
	}
	defer rows.Close()

	if exists(CID) != cid {
		return fmt.Errorf("User with CID %s does not exist", cid)
	}

	// Delete from MSDS
	deleteStatement := `delete from "MSDS" where CID=$1`
	_, err = db.Exec(deleteStatement, cid)
	if err != nil {
		return err
	}

	return nil
}

// ListUsers lists all users in the database
func ListUsers() ([]MSDSCourse, error) {
	Data := []MSDSCourse{}
	db, err := openConnection()
	if err != nil {
		return Data, err
	}
	defer db.Close()

	rows, err := db.Query(`SELECT "CID","CNAME","CPREREQ" FROM "MSDS"`)
	if err != nil {
		return Data, err
	}

	for rows.Next() {
		var CID string
		var CNAME string
		var CPREREQ string
		err = rows.Scan(&CID, &CNAME, &CPREREQ)
		temp := MSDSCourse{CID: CID, CNAME: CNAME, CPREREQ: CPREREQ}
		Data = append(Data, temp)
		if err != nil {
			return Data, err
		}
	}
	defer rows.Close()
	return Data, nil
}

// UpdateUser is for updating an existing user
func UpdateUser(d MSDSCourse) error {
	db, err := openConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	CID := exists(d.CID)
	if CID == "" {
		return errors.New("Course does not exist")
	}
	d.CID = CID
	updateStatement := `update "MSDS" set "CID"=$1, "CNAME"=$2, "CPREREQ"=$3 where "CID"=$1`
	_, err = db.Exec(updateStatement, d.CID, d.CNAME, d.CPREREQ)
	if err != nil {
		return err
	}

	return nil
}
