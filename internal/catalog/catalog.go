package catalog

// BSONType is the raw MongoDB BSON type name, matching the names returned by
// MongoDB's $type aggregation operator.
type BSONType string

const (
	BSONTypeDouble              BSONType = "float64"
	BSONTypeString              BSONType = "string"
	BSONTypeObject              BSONType = "object"
	BSONTypeArray               BSONType = "array"
	BSONTypeBinData             BSONType = "[]byte"
	BSONTypeUndefined           BSONType = "primitive.Undefined" // deprecated
	BSONTypeObjectID            BSONType = "primitive.ObjectID"
	BSONTypeBool                BSONType = "bool"
	BSONTypeDate                BSONType = "DateTime"
	BSONTypeNull                BSONType = "null"
	BSONTypeRegex               BSONType = "primitive.Regex"
	BSONTypeDBPointer           BSONType = "primitive.DBPointer" // deprecated
	BSONTypeJavaScript          BSONType = "primitive.JavaScript"
	BSONTypeSymbol              BSONType = "primitive.Symbol" // deprecated
	BSONTypeJavaScriptWithScope BSONType = "primitive.CodeWithScope"
	BSONTypeInt32               BSONType = "Int32"
	BSONTypeTimestamp           BSONType = "timestamp"
	BSONTypeInt64               BSONType = "int64"
	BSONTypeDecimal128          BSONType = "primitive.Decimal128"
	BSONTypeMinKey              BSONType = "primitive.MinKey"
	BSONTypeMaxKey              BSONType = "primitive.MaxKey"
	BSONTypeUnknown             BSONType = "unknown"
)

// Field represents one column/field in a table/collection
type Field struct {
	Name       string
	RawType    string   // original DB type, e.g. "varchar(255)", "int unsigned"
	NormType   BSONType // normalized cross-DB type
	Nullable   bool
	IsPrimary  bool
	IsUnique   bool
	HasDefault bool
	Extra      string // e.g. "auto_increment", "on update CURRENT_TIMESTAMP"
}

// Stream is one table or collection with its discovered fields
type Stream struct {
	Name      string
	Namespace string // schema name (MySQL db, Postgres schema, Mongo collection db)
	Fields    []Field
	// FieldMap  map[string]*Field // fast lookup by name
}

// func (s *Stream) Field(name string) (*Field, bool) {
// 	f, ok := s.FieldMap[name]
// 	return f, ok
// }

// Catalog is the full discovered schema from a source
type Catalog struct {
	Streams   []*Stream
	StreamMap map[string]*Stream // keyed by "namespace.name"
}

func NewCatalog() *Catalog {
	return &Catalog{StreamMap: make(map[string]*Stream)}
}

func (c *Catalog) Add(s *Stream) {
	c.Streams = append(c.Streams, s)
	c.StreamMap[streamKey(s.Namespace, s.Name)] = s
}

func (c *Catalog) Get(namespace, name string) (*Stream, bool) {
	s, ok := c.StreamMap[streamKey(namespace, name)]
	return s, ok
}

func streamKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "." + name
}

// FilterFields returns a new Stream containing only the fields whose names are
// in keep, preserving the order of keep.  Fields not found in the receiver
// get a TEXT fallback so the table can still be created.
func (s *Stream) FilterFields(keep []string) *Stream {
	byName := make(map[string]Field, len(s.Fields))
	for _, f := range s.Fields {
		byName[f.Name] = f
	}

	out := &Stream{
		Name:      s.Name,
		Namespace: s.Namespace,
		Fields:    make([]Field, 0, len(keep)),
	}

	for _, name := range keep {
		if f, ok := byName[name]; ok {
			out.Fields = append(out.Fields, f)
		} else {
			out.Fields = append(out.Fields, Field{
				Name:      name,
				NormType:  BSONTypeString,
				IsPrimary: name == "_id",
			})
		}
	}

	return out
}
