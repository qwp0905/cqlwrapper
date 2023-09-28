package mapper

import (
	"fmt"
)

type Operator string

const (
	OpEq  = Operator("=")
	OpGt  = Operator(">")
	OpGte = Operator(">=")
	OpLt  = Operator("<")
	OpLte = Operator("<=")
	OpIn  = Operator("IN")
)

type Argument struct {
	field    string
	operator Operator
	arg      any
}

func NewArg(field string, operator Operator, arg any) *Argument {
	return &Argument{field: field, operator: operator, arg: arg}
}

func (a *Argument) Query() string {
	return fmt.Sprintf(`"%s" %s ?`, a.field, a.operator)
}

func (a *Argument) GetField() string {
	return fmt.Sprintf(`"%s"`, a.field)
}

func (a *Argument) GetArg() any {
	return a.arg
}

func (m *Mapper) MappingArgs(o Operator, a any) []*Argument {
	args := []*Argument{}
	v := getValueOf(a)
	for f, i := range m.fields {
		vi := v.Field(i)
		if vi.IsZero() {
			continue
		}
		args = append(args, NewArg(f, o, vi.Interface()))
	}

	return args
}

func (m *Mapper) MappingValues(a any) []*Argument {
	args := []*Argument{}
	v := getValueOf(a)
	for f, i := range m.fields {
		args = append(args, &Argument{
			field: f,
			arg:   v.Field(i).Interface(),
		})
	}

	return args
}

func (m *Mapper) MappingValuesWithSkip(a any) []*Argument {
	args := []*Argument{}
	v := getValueOf(a)
	for f, i := range m.fields {
		vi := v.Field(i)
		if vi.IsZero() {
			continue
		}
		args = append(args, &Argument{
			field: f,
			arg:   vi.Interface(),
		})
	}

	return args
}
