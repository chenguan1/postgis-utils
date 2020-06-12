package models

// metadata of vector data table
type Metadatas struct {
	TableName    string
	Fields       Fields
	Geotype      string
	GeoColumn    string
	GeoSRS       string
	Extent       [4]float64
	FeatureCount int
}
