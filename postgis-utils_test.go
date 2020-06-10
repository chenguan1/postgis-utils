package postgis_utils

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_"github.com/jinzhu/gorm/dialects/postgres"
	"testing"
)

func TestReadMetadatas(t *testing.T) {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", "5432", "postgres", "111111", "dev")

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		t.Error(err)
		return
	}

	meta, err := ReadMetadatas(db, "5ufdxvmgr")
	if err != nil{
		t.Fatal(err)
	}

	fmt.Printf("%#v", meta)



	defer db.Close()
}
