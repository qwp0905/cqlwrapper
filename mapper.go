package cqlwrapper

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func camelToSnake(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func getTypeAndValue(a any) (reflect.Type, reflect.Value) {
	v := reflect.ValueOf(a)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v.Type(), v
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

type Table interface {
	TableName() string
}

func getTableName(a any) string {
	if table, ok := a.(Table); ok {
		return table.TableName()
	}

	return camelToSnake(getSourceType(a).Name())
}

func getFieldName(t *reflect.StructField) string {
	field, ok := t.Tag.Lookup("cql")
	if ok {
		return field
	}

	return camelToSnake(t.Name)
}

func iterateTypeAndValues(a any, f func(t *reflect.StructField, v *reflect.Value, i int) error) error {
	t, v := getTypeAndValue(a)
	for i := 0; i < t.NumField(); i++ {
		ct := t.Field(i)
		cv := v.Field(i)
		if err := f(&ct, &cv, i); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func iterateTypes(a any, f func(t *reflect.StructField, i int)) {
	t := getSourceType(a)
	for i := 0; i < t.NumField(); i++ {
		ct := t.Field(i)
		f(&ct, i)
	}
}

func assignValues(a any, values []any) error {
	return iterateTypeAndValues(a, func(t *reflect.StructField, v *reflect.Value, i int) error {
		if !v.CanSet() {
			return errors.Errorf("cannot assign on field %s", t.Name)
		}
		value := reflect.Indirect(reflect.ValueOf(values[i]))
		if !value.CanConvert(t.Type) {
			return errors.Errorf("cannot convert %s to %s", value.Type().Name(), t.Type.Name())
		}
		v.Set(value.Convert(t.Type))
		return nil
	})
}

func appendValues(a any, values []any) error {
	t, v := getTypeAndValue(a)
	if t.Kind() != reflect.Slice {
		return errors.Errorf("cannot append to %s", t)
	}

	if !v.CanSet() {
		return errors.Errorf("cannot assign on field %s", t.Name())
	}

	t = t.Elem()
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	v.Grow(v.Cap() + 1)
	v.SetLen(v.Cap())
	el := reflect.Indirect(reflect.New(t))
	for i := 0; i < t.NumField(); i++ {
		cv := el.Field(i)
		if !cv.CanSet() {
			return errors.Errorf("cannot assign on field %s", t.Field(i).Name)
		}
		value := reflect.Indirect(reflect.ValueOf(values[i]))
		if !value.CanConvert(t.Field(i).Type) {
			return errors.Errorf("cannot convert %s to %s", value.Type().Name(), t.Field(i).Type.Name())
		}
		cv.Set(value.Convert(t.Field(i).Type))
	}
	v.Index(v.Len() - 1).Set(el.Addr())

	return nil
}

func mappingArgs(o op, a any) []argument {
	args := []argument{}
	iterateTypeAndValues(a, func(t *reflect.StructField, v *reflect.Value, i int) error {
		if v.IsZero() {
			return nil
		}
		args = append(args, argument{
			field:    getFieldName(t),
			operator: o,
			arg:      v.Interface(),
		})
		return nil
	})

	return args
}

func mappingFields(a any) []string {
	fields := []string{}
	iterateTypes(a, func(t *reflect.StructField, i int) {
		fields = append(fields, fmt.Sprintf(`"%s"`, getFieldName(t)))
	})

	return fields
}

func mappingValues(a any) []argument {
	args := []argument{}
	iterateTypeAndValues(a, func(t *reflect.StructField, v *reflect.Value, i int) error {
		args = append(args, argument{
			field: getFieldName(t),
			arg:   v.Interface(),
		})
		return nil
	})
	return args
}

func mappingValuesWithSkip(a any) []argument {
	args := []argument{}
	iterateTypeAndValues(a, func(t *reflect.StructField, v *reflect.Value, i int) error {
		if v.IsZero() {
			return nil
		}
		args = append(args, argument{
			field: getFieldName(t),
			arg:   v.Interface(),
		})
		return nil
	})
	return args
}
