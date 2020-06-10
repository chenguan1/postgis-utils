package postgis_utils

import (
	"fmt"
	"postgis-utils/models"
	"github.com/jinzhu/gorm"
	"strconv"
	"strings"
)

// get table metadatas
func ReadMetadatas(db *gorm.DB, tableName string) (*models.Metadatas, error) {
	meta := &models.Metadatas{}
	meta.TableName = tableName

	if !db.HasTable(tableName){
		return nil, fmt.Errorf("table %v not exist.", tableName)
	}

	// feature count
	type FCount struct {
		Count int64
	}
	var fcount FCount
	err := db.Raw(fmt.Sprintf(`select count(*) from "%v"`, tableName)).Scan(&fcount).Error
	if err != nil{
		return nil, fmt.Errorf("ReadMetadatas get feature count failed: %v", err)
	}
	if fcount.Count == 0{
		return nil,fmt.Errorf("ReadMetadatas feature count is 0.")
	}
	meta.FeatureCount = int(fcount.Count)

	// 字段信息
	sqlstr := fmt.Sprintf(`SELECT col_description(a.attrelid,a.attnum) as comment,
format_type(a.atttypid,a.atttypmod) as type,a.attname as name, a.attnotnull as notnull   
FROM pg_class as c,pg_attribute as a where c.relname = '%v' and a.attrelid = c.oid and a.attnum>0`,tableName)

	type FieldInfo struct {
		Name string
		Type string
	}
	fs := make([]FieldInfo,0,32)
	err = db.Raw(sqlstr).Scan(&fs).Error
	if err != nil{
		return nil, fmt.Errorf("ReadMetadatas get fields info failed: %v", err)
	}

	meta.Fields = models.NewFields()
	for _,v := range fs {
		meta.Fields.Set(v.Name,v.Type)
		if strings.Contains(v.Type, "geometry"){
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
	if err != nil{
		return nil, fmt.Errorf("ReadMetadatas get srs and geotype info failed: %v", err)
	}

	meta.GeoSRS = sg.St_srid
	meta.Geotype = sg.Geometrytype

	// meta.Extent
	type St_extent struct {
		St_extent string
	}
	var se St_extent
	sqlstr = fmt.Sprintf(`select ST_Extent("%v") from "%v"`, meta.GeoColumn,meta.TableName)
	err = db.Raw(sqlstr).Scan(&se).Error
	if err != nil{
		return nil, fmt.Errorf("ReadMetadatas get srs and geotype info failed: %v", err)
	}
	strsub := se.St_extent[4:len(se.St_extent)-1]
	ptStrs := strings.Split(strsub,",")
	if len(ptStrs) != 2{
		return nil, fmt.Errorf("ReadMetadatas get extent failed: %v", strsub)
	}

	for i := 0; i < 2; i++{
		nums := strings.Split(ptStrs[i], " ")
		meta.Extent[i*2],err = strconv.ParseFloat(nums[0], 64)
		if err != nil{
			return nil, fmt.Errorf("ReadMetadatas get extent value failed: %v", strsub)
		}

		meta.Extent[i*2+1],err = strconv.ParseFloat(nums[1], 64)
		if err != nil{
			return nil, fmt.Errorf("ReadMetadatas get extent value failed: %v", strsub)
		}
	}

	return meta, nil
}

// query circle
func QueryCircel(db *gorm.DB, x, y, r float64)  {
	
}

