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
	Name      string
	NormType  BSONType // normalized cross-DB type
	DestType  string   // explicit destination PostgreSQL type, e.g. "TIMESTAMPTZ"; overrides NormType mapping
	Nullable  bool
	IsPrimary bool
	IsUnique  bool
	// HasDefault bool
	// Extra      string // e.g. "auto_increment", "on update CURRENT_TIMESTAMP"
}

// Stream is one table or collection with its discovered fields
type Stream struct {
	Name      string
	Namespace string // schema name (MySQL db, Postgres schema, Mongo collection db)
	Fields    []Field
	FieldMap  map[string]Field // fast lookup by name
}

// AddField appends f to Fields and registers it in FieldMap.
func (s *Stream) AddField(f Field) {
	if s.FieldMap == nil {
		s.FieldMap = make(map[string]Field)
	}
	s.Fields = append(s.Fields, f)
	s.FieldMap[f.Name] = f
}

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

// FieldSpec pairs a field name with an optional explicit PostgreSQL destination
// type. Leave PgType empty to have the type inferred from the discovered BSON type.
type FieldSpec struct {
	Name   string
	PgType string // e.g. "TIMESTAMPTZ", "BIGINT", "JSONB"; empty = auto
}

// FilterFields returns a new Stream containing only the fields whose names are
// in specs, preserving their order. Fields not found in the receiver get a TEXT
// fallback. DestType is set on each field when PgType is non-empty.
func (s *Stream) FilterFields(specs []FieldSpec) *Stream {
	out := &Stream{
		Name:      s.Name,
		Namespace: s.Namespace,
	}

	for _, spec := range specs {
		f, ok := s.FieldMap[spec.Name]
		if !ok {
			continue
		}
		f.DestType = spec.PgType
		out.AddField(f)
	}

	return out
}
