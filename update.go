package cqlwrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type UpdateQueryBuilder struct {
	*queryRecover
	session     *Session
	ctx         context.Context
	table       string
	consistency gocql.Consistency
	args        []argument
	fields      []argument
}

func newUpdateQueryBuilder(ctx context.Context, session *Session) *UpdateQueryBuilder {
	return &UpdateQueryBuilder{
		queryRecover: recoverer(),
		session:      session,
		consistency:  session.consistency,
		ctx:          ctx,
		args:         []argument{},
		fields:       []argument{},
	}
}

func (qb *UpdateQueryBuilder) Set(a any) *UpdateQueryBuilder {
	defer qb.recover()
	if qb.table == "" {
		qb.table = getTableName(a)
	}
	qb.args = mappingValuesWithSkip(a)

	return qb
}

func (qb *UpdateQueryBuilder) bind(o op, a any) *UpdateQueryBuilder {
	defer qb.recover()
	if qb.table == "" {
		qb.table = getTableName(a)
	}
	qb.args = mappingArgs(o, a)
	return qb
}

func (qb *UpdateQueryBuilder) Consistency(co gocql.Consistency) *UpdateQueryBuilder {
	qb.consistency = co
	return qb
}

func (qb *UpdateQueryBuilder) Where(a any) *UpdateQueryBuilder {
	return qb.bind(opEq, a)
}

func (qb *UpdateQueryBuilder) WhereGt(a any) *UpdateQueryBuilder {
	return qb.bind(opGt, a)
}

func (qb *UpdateQueryBuilder) WhereGte(a any) *UpdateQueryBuilder {
	return qb.bind(opGte, a)
}

func (qb *UpdateQueryBuilder) WhereLt(a any) *UpdateQueryBuilder {
	return qb.bind(opLt, a)
}

func (qb *UpdateQueryBuilder) WhereLte(a any) *UpdateQueryBuilder {
	return qb.bind(opLte, a)
}

func (qb *UpdateQueryBuilder) WhereIn(field string, args any) *UpdateQueryBuilder {
	qb.args = append(qb.args, argument{
		field:    field,
		operator: opIn,
		arg:      args,
	})
	return qb
}

func (qb *UpdateQueryBuilder) getQuery() string {
	fields := []string{}
	vars := []string{}
	for _, field := range qb.fields {
		fields = append(fields, field.getField())
		vars = append(vars, "?")
	}
	query := fmt.Sprintf(
		`UPDATE %s (%s) VALUES (%s)`,
		qb.table,
		strings.Join(fields, ","),
		strings.Join(vars, ","),
	)

	if len(qb.args) > 0 {
		where := []string{}
		for _, arg := range qb.args {
			where = append(where, arg.query())
		}
		query += fmt.Sprintf(` WHERE %s`, strings.Join(where, " AND "))
	}

	return query
}

func (qb *UpdateQueryBuilder) getArgs() []any {
	out := []any{}
	for _, field := range qb.fields {
		out = append(out, field.arg)
	}
	for _, arg := range qb.args {
		out = append(out, arg.arg)
	}
	return out
}

func (qb *UpdateQueryBuilder) Exec() error {
	if qb.err != nil {
		return qb.err
	}

	return errors.WithStack(qb.session.
		Query(qb.getQuery()).
		Bind(qb.getArgs()...).
		Consistency(qb.consistency).
		WithContext(qb.ctx).
		Exec())
}
