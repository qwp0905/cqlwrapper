package cqlwrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type InsertQueryBuilder struct {
	*queryRecover
	session     *Session
	ctx         context.Context
	consistency gocql.Consistency
	ifNotExists bool
	args        []argument
	into        string
}

func newInsertQueryBuilder(ctx context.Context, session *Session) *InsertQueryBuilder {
	return &InsertQueryBuilder{
		queryRecover: recoverer(),
		ctx:          ctx,
		consistency:  session.consistency,
		session:      session,
		ifNotExists:  false,
		args:         []argument{},
	}
}

func (qb *InsertQueryBuilder) Consistency(co gocql.Consistency) *InsertQueryBuilder {
	qb.consistency = co
	return qb
}

func (qb *InsertQueryBuilder) IfNotExists() *InsertQueryBuilder {
	qb.ifNotExists = true
	return qb
}

func (qb *InsertQueryBuilder) Values(a any) *InsertQueryBuilder {
	defer qb.recover()
	if qb.into == "" {
		qb.into = getTableName(a)
	}
	qb.args = mappingValues(a)
	return qb
}

func (qb *InsertQueryBuilder) getQuery() string {
	fields := []string{}
	questionMarks := []string{}
	for _, arg := range qb.args {
		fields = append(fields, arg.getField())
		questionMarks = append(questionMarks, "?")
	}
	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		qb.into,
		strings.Join(fields, ","),
		strings.Join(questionMarks, ","),
	)

	if qb.ifNotExists {
		query += " IF NOT EXISTS"
	}

	return query
}

func (qb *InsertQueryBuilder) getArgs() []any {
	out := []any{}
	for _, arg := range qb.args {
		out = append(out, arg.arg)
	}
	return out
}

func (qb *InsertQueryBuilder) Exec() error {
	if err := qb.error(); err != nil {
		return err
	}

	return errors.WithStack(qb.session.
		Query(qb.getQuery()).
		Bind(qb.getArgs()...).
		WithContext(qb.ctx).
		Consistency(qb.consistency).
		Exec())
}
