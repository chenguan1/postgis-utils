package postgis_utils

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"io/ioutil"
	"os"
	"strings"
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
	defer db.Close()

	meta, err := ReadMetadatas(db, "5ufdxvmgr")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%#v", meta)

}

func TestQueryCircel(t *testing.T) {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", "5432", "postgres", "111111", "dev")

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	geojsons, err := QueryCircel(db, "5ufdxvmgr", 120.36965179, 31.48193359, 100000)
	if err != nil {
		t.Error(err)
		return
	}

	ioutil.WriteFile("data/circle.txt", []byte(strings.Join(geojsons, "\n")), os.ModePerm)

	fmt.Println(len(geojsons))
}

func TestQueryRect(t *testing.T) {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", "5432", "postgres", "111111", "dev")

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	geojsons, err := QueryRect(db, "5ufdxvmgr", 118.36965179, 31.48193359, 120.36965179, 32.48193359)
	if err != nil {
		println(err)
		t.Error(err)
		return
	}

	ioutil.WriteFile("data/rect.txt", []byte(strings.Join(geojsons, "\n")), os.ModePerm)

	fmt.Println(len(geojsons))
}

func TestQueryPolygon(t *testing.T) {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", "5432", "postgres", "111111", "dev")

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	geojsons, err := QueryPolygon(db, "5ufdxvmgr", []float64{118.36965179, 31.48193359, 120.36965179, 31.48193359, 120.36965179, 32.48193359, 119.36965179, 32.48193359, 118.36965179, 31.48193359})
	if err != nil {
		println(err)
		t.Error(err)
		return
	}

	ioutil.WriteFile("data/polygon.txt", []byte(strings.Join(geojsons, "\n")), os.ModePerm)

	fmt.Println(len(geojsons))
}

func TestQueryFiled(t *testing.T) {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", "5432", "postgres", "111111", "dev")

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	geojsons, err := QueryFiled(db, "5ufdxvmgr", "name", "%北京%", "like")
	if err != nil {
		t.Error(err)
		return
	}

	ioutil.WriteFile("data/field.txt", []byte(strings.Join(geojsons, "\n")), os.ModePerm)

	fmt.Println(len(geojsons))
}

func TestQueryFuzzy(t *testing.T) {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", "5432", "postgres", "111111", "dev")

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	geojsons, err := QueryFuzzy(db, "5ufdxvmgr", []string{"fclass", "name"}, "crossing")
	if err != nil {
		t.Error(err)
		return
	}

	ioutil.WriteFile("data/fuzzy.txt", []byte(strings.Join(geojsons, "\n")), os.ModePerm)

	fmt.Println(len(geojsons))
}

func TestFeatureDelete(t *testing.T) {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", "5432", "postgres", "111111", "dev")

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	err = FeatureDelete(db, "5ufdxvmgr", "gid", "3")
	if err != nil {
		t.Error(err)
		return
	}

}


func TestFeatureInsert(t *testing.T) {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", "5432", "postgres", "111111", "dev")

	db, err := gorm.Open("postgres", conn)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	fg := `{"type": "Feature", "geometry": {"type":"MultiPoint","coordinates":[[118.7790457,32.0601774]]}, "properties": {"gid": 293, "osm_id": "32918369", "code": 5201, "fclass": "traffic_signals", "name": "gray"}}`
	err = FeatureInsert(db, "5ufdxvmgr", fg)
	if err != nil {
		t.Error(err)
		return
	}
}