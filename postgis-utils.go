package postgis_utils

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/paulmach/orb/geojson"
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

	meta.KeyColumn = "gid"

	return meta, nil
}

// query circle, return geojson
func QueryCircel(db *gorm.DB, tableName string, x, y, r float64) ([]string, error) {
	sqlstr := fmt.Sprintf(`
select st_asgeojson(t.*) as geojson
from "%v" as t 
where ST_DWithin(
ST_Transform(ST_GeomFromText('POINT(%v %v)',4326),26986),
ST_Transform(t.geom,26986),
%v)`, tableName, x, y, r)

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
	sqlstr := fmt.Sprintf(
		`
select st_asgeojson(t.*) as geojson 
from "%v" as t 
where ST_Intersects(t.geom, 'SRID=4326;POLYGON((%v %v,%v %v,%v %v,%v %v))')`,
		tableName, minx, miny, minx, maxy, maxx, maxy, minx, miny)

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

	sqlstr := fmt.Sprintf(
		`select st_asgeojson(t.*) as geojson from "%v" as t where ST_Intersects(t.geom,'SRID=4326;POLYGON((%v))')`,
		tableName, ptlist)

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
	sqlstr := fmt.Sprintf(`select st_asgeojson(t.*) as geojson from "%v" as t where "%v" %v '%v'`, tableName, fieldName, op, value)

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
func QueryFuzzy(db *gorm.DB, tableName string, keyword string) ([]string, error) {
	// metadata
	metadata, err := ReadMetadatas(db, tableName)
	if err != nil {
		return nil, fmt.Errorf("QueryFuzzy readmatadatas failed: %v", err)
	}

	keys := metadata.Fields.Keys()
	conditions := ``
	for _, k := range keys {
		fieldType, _ := metadata.Fields.Get(k)
		if strings.Contains(fieldType, "character") {
			if len(conditions) > 0 {
				conditions = conditions + "or "
			}
			conditions = conditions + fmt.Sprintf(`"%v" like '%v' `, k, "%"+keyword+"%")
		}
	}

	sqlstr := fmt.Sprintf(`select st_asgeojson(t.*) as geojson from "%v" as t where %v`, tableName, conditions)

	//fmt.Println(sqlstr)

	type GJson struct {
		Geojson []byte
	}

	gjs := make([]GJson, 0)

	err = db.Raw(sqlstr).Scan(&gjs).Error
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
func FeatureDelete(db *gorm.DB, tableName string, featureId interface{}) error {
	// metadata
	metadata, err := ReadMetadatas(db, tableName)
	if err != nil {
		return fmt.Errorf("FeatureDelete readmatadatas failed: %v", err)
	}

	sqlstr := fmt.Sprintf(`delete from "%v" where "%v" = '%v'`, tableName, metadata.KeyColumn, featureId)
	err = db.Exec(sqlstr).Error
	if err != nil {
		return fmt.Errorf("FeatureDelete failed: %v", err)
	}

	return nil
}

// Feature insert
func FeatureInsert(db *gorm.DB, tableName, featureGeojson string) error {
	feature, err := geojson.UnmarshalFeature([]byte(featureGeojson))
	if err != nil {
		return fmt.Errorf("FeatureInsert unmarshal geojson failed: %v", err)
	}

	// metadata
	metadata, err := ReadMetadatas(db, tableName)
	if err != nil {
		return fmt.Errorf("FeatureInsert readmatadatas failed: %v", err)
	}

	// => wkt
	wktstr := wkt.MarshalString(feature.Geometry)

	// propertis
	setfiled := ""
	setvalue := ""
	properties := map[string]interface{}(feature.Properties)
	for _, key := range metadata.Fields.Keys() {
		if key == metadata.KeyColumn {
			continue
		}
		if v, ok := properties[key]; ok {
			setfiled = setfiled + key + ","
			setvalue = setvalue + fmt.Sprintf("'%v',", v)
		}
	}

	setfiled = setfiled + metadata.GeoColumn
	setvalue = setvalue + fmt.Sprintf("st_geomfromtext('%v',%v)", wktstr, metadata.GeoSRS)

	sqlfmt := `insert into "%s" (%s) values (%s)`
	sqlstr := fmt.Sprintf(sqlfmt, tableName, setfiled, setvalue)

	//fmt.Println(sqlstr)

	if err = db.Exec(sqlstr).Error; err != nil {
		return fmt.Errorf("FeatureInsert insert feature failed: %v", err)
	}

	return nil
}

// Feature update
func FeatureUpdate(db *gorm.DB, tableName string, featureId interface{}, featureGeojson string) error {
	feature, err := geojson.UnmarshalFeature([]byte(featureGeojson))
	if err != nil {
		return fmt.Errorf("FeatureUpdate unmarshal geojson failed: %v", err)
	}

	// metadata
	metadata, err := ReadMetadatas(db, tableName)
	if err != nil {
		return fmt.Errorf("FeatureUpdate readmatadatas failed: %v", err)
	}

	// => wkt
	wktstr := wkt.MarshalString(feature.Geometry)

	// propertis
	setvalue := ""
	properties := map[string]interface{}(feature.Properties)
	for _, key := range metadata.Fields.Keys() {
		if v, ok := properties[key]; ok {
			setvalue = setvalue + fmt.Sprintf(`"%v"='%v',`, key, v)
		}
	}

	setvalue = setvalue + fmt.Sprintf(`"%v" = st_geomfromtext('%v',%v)`, metadata.GeoColumn, wktstr, metadata.GeoSRS)

	sqlstr := fmt.Sprintf(`update "%s" set %s where %s = %v`, tableName, setvalue, metadata.KeyColumn, featureId)

	//fmt.Println(sqlstr)

	if err = db.Exec(sqlstr).Error; err != nil {
		return fmt.Errorf("FeatureUpdate update feature failed: %v", err)
	}

	return nil
}

// Generate MVT(Mapbox Vector Tile), vector table only surport epsg:4326 and epsg:3857
func GenerateMVT(db *gorm.DB, tableName, layerName string, fields []string, zoom, x, y int) ([]byte, error) {
	var err error
	var metadata *models.Metadatas
	if fields == nil || len(fields) == 0 {
		metadata, err = ReadMetadatas(db, tableName)
		if err != nil {
			return nil, fmt.Errorf("GenerateMVT readmatadatas failed: %v", err)
		}
		metaFields := metadata.Fields.Keys()

		fields = make([]string, 0, len(metaFields)+2)
		for _,v := range metaFields{
			if v == metadata.GeoColumn{ // skip geom column
				continue
			}
			fields = append(fields, v)
		}
	}

	// fields remove geom
	for i,v := range fields{
		if v == "geom"{ // skip geom
			fields = append(fields[:i], fields[i+1:]...)
			break
		}
	}

	// to string
	flds := `"` + strings.Join(fields, `","`) + `"`

	//fmt.Println(zoom,x,y)

	minx, miny := TileUl_4326(zoom, x, y)
	maxx, maxy := TileUl_4326(zoom, x+1, y+1)

	if layerName == "" {
		layerName = tableName
	}

	sqltmp := `
WITH mvtgeom AS
	(SELECT ST_AsMVTGeom(ST_Transform(%v,3857),ST_TileEnvelope(%v,%v,%v)) AS geom, %v
	FROM "%v" 
	WHERE ST_Intersects(%v,ST_MakeEnvelope(%v,%v,%v,%v, 4326)))
	SELECT ST_AsMvt(mvtgeom.*,'%v') AS mvt
	FROM mvtgeom
`

	sqlstr := fmt.Sprintf(sqltmp, "geom", zoom, x, y, flds, tableName, "geom", minx, miny, maxx, maxy, layerName)
	//fmt.Println(sqlstr)

	var gi struct{
		Mvt []byte
	}

	err = db.Raw(sqlstr).Scan(&gi).Error
	if err != nil {
		return nil, fmt.Errorf("GenerateMVT query tile failed: %v", err)
	}
	return gi.Mvt, nil
}
