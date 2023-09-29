package mapper

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var matchFirstCap = regexp.MustCompile(`(.)([A-Z][a-z]+)`)
var matchAllCap = regexp.MustCompile(`([a-z0-9])([A-Z])`)

func camelToSnake(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

type Table interface {
	TableName() string
}

type Mapper struct {
	fields map[string]int
	table  string
}

func New(model any) *Mapper {
	t := getSourceType(model)
	fields := make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		fields[getFieldName(t.Field(i))] = i
	}

	return &Mapper{fields: fields, table: getTableName(model)}
}

func getTableName(model any) string {
	if table, ok := model.(Table); ok {
		return table.TableName()
	}

	t := getSourceType(model)
	v := reflect.New(t)
	if v.Type().Implements(reflect.TypeOf((*Table)(nil)).Elem()) {
		return v.MethodByName("TableName").
			Call([]reflect.Value{})[0].
			String()
	}

	return camelToSnake(t.Name())
}

func (m *Mapper) GetTable() string {
	return m.table
}

func (m *Mapper) GetFields() []string {
	fields := []string{}
	for k := range m.fields {
		fields = append(fields, k)
	}
	return fields
}

func getSourceType(a any) reflect.Type {
	t := reflect.TypeOf(a)
	for t.Kind() == reflect.Slice ||
		t.Kind() == reflect.Ptr ||
		t.Kind() == reflect.Array {
		t = t.Elem()
	}
	return t
}
func getValueOf(a any) reflect.Value {
	v := reflect.ValueOf(a)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v
}

func getTypeAndValue(a any) (reflect.Type, reflect.Value) {
	v := getValueOf(a)
	return v.Type(), v
}

func getFieldName(t reflect.StructField) string {
	field, ok := t.Tag.Lookup("cql")
	if ok {
		return field
	}

	return camelToSnake(t.Name)
}

func (m *Mapper) assign(v reflect.Value, t reflect.StructField, a any) (err error) {
	defer func() {
		if r, ok := recover().(error); ok && r != nil {
			err = r
		}
	}()
	if !v.CanSet() {
		return errors.Errorf("cannot set on field %s", t.Name)
	}

	indirect := reflect.Indirect(reflect.ValueOf(a))
	if !indirect.CanConvert(t.Type) {
		return errors.Errorf(
			"cannot convert %s to %s",
			indirect.Type().Name(),
			t.Type.Name(),
		)
	}

	v.Set(indirect.Convert(t.Type))
	return nil
}

func (m *Mapper) AssignValues(a any, fields []string, values []any) (err error) {
	defer func() {
		if r, ok := recover().(error); ok && r != nil {
			err = r
		}
	}()
	t, v := getTypeAndValue(a)
	for i, field := range fields {
		sourceIndex, ok := m.fields[field]
		if !ok {
			return errors.Errorf("unknown field %s", field)
		}
		if err := m.assign(
			v.Field(sourceIndex),
			t.Field(sourceIndex),
			values[i],
		); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mapper) AppendValues(a any, fields []string, values []any) (err error) {
	defer func() {
		if r, ok := recover().(error); ok && r != nil {
			err = r
		}
	}()
	t, v := getTypeAndValue(a)
	if t.Kind() != reflect.Slice {
		return errors.Errorf("cannot append to %s", t)
	}
	if !v.CanSet() {
		return errors.Errorf("cannot assign on %s", t.Name())
	}
	t = t.Elem()
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v.Grow(1)
	v.SetLen(v.Len() + 1)

	el := reflect.Indirect(reflect.New(t))
	for i, field := range fields {
		sourceIndex, ok := m.fields[field]
		if !ok {
			return errors.Errorf("unknown field %s", field)
		}
		if err := m.assign(
			el.Field(sourceIndex),
			t.Field(sourceIndex),
			values[i],
		); err != nil {
			return err
		}
	}

	v.Index(v.Len() - 1).Set(el.Addr())

	return nil
}
