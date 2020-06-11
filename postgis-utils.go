package postgis_utils

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"postgis-utils/models"
	"strconv"
	"strings"
)

// get table metadatas
func ReadMetadatas(db *gorm.DB, tableName string) (*models.Metadatas, error) {
	meta := &models.Metadatas{}
	meta.TableName = tableName

	if !db.HasTable(tableName) {
		return nil, fmt.Errorf("table %v not exist.", tableName)
	}

	// feature count
	type FCount struct {
		Count int64
	}
	var fcount FCount
	err := db.Raw(fmt.Sprintf(`select count(*) from "%v"`, tableName)).Scan(&fcount).Error
	if err != nil {
		return nil, fmt.Errorf("ReadMetadatas get feature count failed: %v", err)
	}
	if fcount.Count == 0 {
		return nil, fmt.Errorf("ReadMetadatas feature count is 0.")
	}
	meta.FeatureCount = int(fcount.Count)

	// 字段信息
	sqlstr := fmt.Sprintf(`SELECT col_description(a.attrelid,a.attnum) as comment,
format_type(a.atttypid,a.atttypmod) as type,a.attname as name, a.attnotnull as notnull   
FROM pg_class as c,pg_attribute as a where c.relname = '%v' and a.attrelid = c.oid and a.attnum>0`, tableName)

	type FieldInfo struct {
		Name string
		Type string
	}
	fs := make([]FieldInfo, 0, 32)
	err = db.Raw(sqlstr).Scan(&fs).Error
	if err != nil {
		return nil, fmt.Errorf("ReadMetadatas get fields info failed: %v", err)
	}

	meta.Fields = models.NewFields()
	for _, v := range fs {
		meta.Fields.Set(v.Name, v.Type)
		if strings.Contains(v.Type, "geometry") {
			meta.GeoColumn = v.Name
		}
	}

	// 投影信息和矢量类型
	type SRSAndGeotype struct {
		St_srid      string
		Geometrytype string
	}
	var sg SRSAndGeotype
	sqlstr = fmt.Sprintf(`select ST_SRID(%v), GeometryType(%v) from "%v" limit 1`,
		meta.GeoColumn, meta.GeoColumn, meta.TableName)
	err = db.Raw(sqlstr).Scan(&sg).Error
	if err != nil {
		return nil, fmt.Errorf("ReadMetadatas get srs and geotype info failed: %v", err)
	}

	meta.GeoSRS = sg.St_srid
	meta.Geotype = sg.Geometrytype

	// meta.Extent
	type St_extent struct {
		St_extent string
	}
	var se St_extent
	sqlstr = fmt.Sprintf(`select ST_Extent("%v") from "%v"`, meta.GeoColumn, meta.TableName)
	err = db.Raw(sqlstr).Scan(&se).Error
	if err != nil {
		return nil, fmt.Errorf("ReadMetadatas get srs and geotype info failed: %v", err)
	}
	strsub := se.St_extent[4 : len(se.St_extent)-1]
	ptStrs := strings.Split(strsub, ",")
	if len(ptStrs) != 2 {
		return nil, fmt.Errorf("ReadMetadatas get extent failed: %v", strsub)
	}

	for i := 0; i < 2; i++ {
		nums := strings.Split(ptStrs[i], " ")
		meta.Extent[i*2], err = strconv.ParseFloat(nums[0], 64)
		if err != nil {
			return nil, fmt.Errorf("ReadMetadatas get extent value failed: %v", strsub)
		}

		meta.Extent[i*2+1], err = strconv.ParseFloat(nums[1], 64)
		if err != nil {
			return nil, fmt.Errorf("ReadMetadatas get extent value failed: %v", strsub)
		}
	}

	return meta, nil
}

// query circle, return geojson
func QueryCircel(db *gorm.DB, tableName string, x, y, r float64) ([]string, error) {
	sqlstr := fmt.Sprintf(`select st_asgeojson(t.*) as geojson
		from "%v" as t 
		where ST_DWithin(
			ST_Transform(ST_GeomFromText('POINT(%v %v)',4326),26986),
			ST_Transform(t.geom,26986),
			%v
		)`, tableName, x, y, r)

	type GJson struct {
		Geojson []byte
	}

	gjs := make([]GJson, 0)

	err := db.Raw(sqlstr).Scan(&gjs).Error
	if err != nil {
		return nil, fmt.Errorf("QueryCircel query db failed: %v", err)
	}

	geojsons := make([]string, 0)
	for _, v := range gjs {
		geojsons = append(geojsons, string(v.Geojson))
	}

	return geojsons, nil
}

// query rectangle
func QueryRect(db *gorm.DB, tableName string, minx, miny, maxx, maxy float64) ([]string, error) {
	sqlstr := fmt.Sprintf(`select st_asgeojson(t.*) as geojson 
from "%v" as t 
where ST_Intersects(t.geom, 
 'SRID=4326;POLYGON((%v %v,%v %v,%v %v,%v %v))')
`, tableName, minx, miny, minx, maxy, maxx, maxy, minx, miny)

	type GJson struct {
		Geojson []byte
	}

	gjs := make([]GJson, 0)

	err := db.Raw(sqlstr).Scan(&gjs).Error
	if err != nil {
		return nil, fmt.Errorf("QueryRect query db failed: %v", err)
	}

	geojsons := make([]string, 0)
	for _, v := range gjs {
		geojsons = append(geojsons, string(v.Geojson))
	}

	return geojsons, nil
}

// query polygon
func QueryPolygon(db *gorm.DB, tableName string, pts []float64) ([]string, error) {
	if len(pts)%2 != 0 {
		return nil, fmt.Errorf("QueryPolygon pts length not correct.")
	}

	ptlist := ""
	for i, v := range pts {
		ptlist = ptlist + fmt.Sprintf("%v", v)
		if i%2 == 0 {
			ptlist = ptlist + " "
		} else if i != len(pts)-1 {
			ptlist = ptlist + ","
		}
	}

	sqlstr := fmt.Sprintf(`select st_asgeojson(t.*) as geojson from "%v" as t
where ST_Intersects(t.geom,'SRID=4326;POLYGON((%v))')`, tableName, ptlist)

	type GJson struct {
		Geojson []byte
	}

	gjs := make([]GJson, 0)

	err := db.Raw(sqlstr).Scan(&gjs).Error
	if err != nil {
		return nil, fmt.Errorf("QueryRect query db failed: %v", err)
	}

	geojsons := make([]string, 0)
	for _, v := range gjs {
		geojsons = append(geojsons, string(v.Geojson))
	}

	return geojsons, nil
}

// query by filed value
func QueryFiled(db *gorm.DB, tableName, fieldName string, value interface{}, op string) ([]string, error) {
	sqlstr := fmt.Sprintf(`select st_asgeojson(t.*) as geojson
		from "%v" as t 
		where "%v" %v '%v'`, tableName, fieldName, op, value)

	type GJson struct {
		Geojson []byte
	}

	gjs := make([]GJson, 0)

	err := db.Raw(sqlstr).Scan(&gjs).Error
	if err != nil {
		return nil, fmt.Errorf("QueryFiled query db failed: %v", err)
	}

	geojsons := make([]string, 0)
	for _, v := range gjs {
		geojsons = append(geojsons, string(v.Geojson))
	}

	return geojsons, nil
}

// Fuzzy query
func QueryFuzzy(db *gorm.DB, tableName string, fields []string, keyword string) ([]string, error) {
	conditions := ``
	for i, v := range fields{
		conditions = conditions + fmt.Sprintf(`"%v" like '%v' `, v, "%" + keyword + "%")
		if i != len(fields)-1 {
			conditions = conditions + "or "
		}
	}

	sqlstr := fmt.Sprintf(`select st_asgeojson(t.*) as geojson
		from "%v" as t 
		where %v`, tableName, conditions)

	type GJson struct {
		Geojson []byte
	}

	gjs := make([]GJson, 0)

	err := db.Raw(sqlstr).Scan(&gjs).Error
	if err != nil {
		return nil, fmt.Errorf("QueryFuzzy query db failed: %v", err)
	}

	geojsons := make([]string, 0)
	for _, v := range gjs {
		geojsons = append(geojsons, string(v.Geojson))
	}

	return geojsons, nil
}

// Feature delete
func FeatureDelete(db *gorm.DB, tableName, idFieldName string, idValue interface{}) error {
	return nil
}

// Feature insert
func FeatureInsert(db *gorm.DB, tableName, featureGeojson string) error {
	return nil
}

// Feature update
func FeatureUpdate(db *gorm.DB, tableName, idFieldName string, idValue interface{}, featureGeojson string) error {
	return nil
}