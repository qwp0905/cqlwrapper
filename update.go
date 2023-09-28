package cqlwrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/qwp0905/cqlwrapper/internal/mapper"
)

type UpdateQueryBuilder struct {
	*queryRecover
	session     *Session
	ctx         context.Context
	model       *mapper.Mapper
	consistency gocql.Consistency
	args        []*mapper.Argument
	fields      []*mapper.Argument
}

func newUpdateQueryBuilder(ctx context.Context, session *Session) *UpdateQueryBuilder {
	return &UpdateQueryBuilder{
		queryRecover: recoverer(),
		session:      session,
		consistency:  session.consistency,
		ctx:          ctx,
		args:         []*mapper.Argument{},
		fields:       []*mapper.Argument{},
		model:        nil,
	}
}

func (qb *UpdateQueryBuilder) Set(a any) *UpdateQueryBuilder {
	defer qb.recover()
	if qb.model == nil {
		qb.model = mapper.New(a)
	}
	qb.args = qb.model.MappingValuesWithSkip(a)

	return qb
}

func (qb *UpdateQueryBuilder) bind(o mapper.Operator, a any) *UpdateQueryBuilder {
	defer qb.recover()
	if qb.model == nil {
		qb.model = mapper.New(a)
	}
	qb.args = append(qb.args, qb.model.MappingArgs(o, a)...)
	return qb
}

func (qb *UpdateQueryBuilder) Consistency(co gocql.Consistency) *UpdateQueryBuilder {
	qb.consistency = co
	return qb
}

func (qb *UpdateQueryBuilder) Where(a any) *UpdateQueryBuilder {
	return qb.bind(mapper.OpEq, a)
}

func (qb *UpdateQueryBuilder) WhereGt(a any) *UpdateQueryBuilder {
	return qb.bind(mapper.OpGt, a)
}

func (qb *UpdateQueryBuilder) WhereGte(a any) *UpdateQueryBuilder {
	return qb.bind(mapper.OpGte, a)
}

func (qb *UpdateQueryBuilder) WhereLt(a any) *UpdateQueryBuilder {
	return qb.bind(mapper.OpLt, a)
}

func (qb *UpdateQueryBuilder) WhereLte(a any) *UpdateQueryBuilder {
	return qb.bind(mapper.OpLte, a)
}

func (qb *UpdateQueryBuilder) WhereIn(field string, args any) *UpdateQueryBuilder {
	qb.args = append(qb.args, mapper.NewArg(field, mapper.OpIn, args))
	return qb
}

func (qb *UpdateQueryBuilder) getQuery() string {
	fields := []string{}
	for _, field := range qb.fields {
		fields = append(fields, field.GetField())
	}
	query := fmt.Sprintf(
		`UPDATE %s (%s) VALUES (%s)`,
		qb.model.GetTable(),
		strings.Join(fields, ","),
		questionMarks(len(fields)),
	)

	if len(qb.args) > 0 {
		where := []string{}
		for _, arg := range qb.args {
			where = append(where, arg.Query())
		}
		query += fmt.Sprintf(` WHERE %s`, strings.Join(where, " AND "))
	}

	return query
}

func (qb *UpdateQueryBuilder) getArgs() []any {
	out := []any{}
	for _, field := range qb.fields {
		out = append(out, field.GetArg())
	}
	for _, arg := range qb.args {
		out = append(out, arg.GetArg())
	}
	return out
}

func (qb *UpdateQueryBuilder) Exec() error {
	if err := qb.error(); err != nil {
		return err
	}

	return errors.WithStack(qb.session.
		Query(qb.getQuery()).
		Bind(qb.getArgs()...).
		Consistency(qb.consistency).
		WithContext(qb.ctx).
		Exec())
}
