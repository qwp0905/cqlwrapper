package cqlwrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/qwp0905/cqlwrapper/internal/mapper"
)

type InsertQueryBuilder struct {
	*queryRecover
	session     *Session
	ctx         context.Context
	consistency gocql.Consistency
	ifNotExists bool
	args        []*mapper.Argument
	model       *mapper.Mapper
}

func newInsertQueryBuilder(ctx context.Context, session *Session) *InsertQueryBuilder {
	return &InsertQueryBuilder{
		queryRecover: recoverer(),
		ctx:          ctx,
		consistency:  session.consistency,
		session:      session,
		ifNotExists:  false,
		args:         []*mapper.Argument{},
		model:        nil,
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
	if qb.model == nil {
		qb.model = mapper.New(a)
	}
	qb.args = qb.model.MappingValues(a)
	return qb
}

func (qb *InsertQueryBuilder) getQuery() string {
	fields := []string{}
	for _, arg := range qb.args {
		fields = append(fields, arg.GetField())
	}
	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		qb.model.GetTable(),
		strings.Join(fields, ","),
		questionMarks(len(fields)),
	)

	if qb.ifNotExists {
		query += " IF NOT EXISTS"
	}

	return query
}

func (qb *InsertQueryBuilder) getArgs() []any {
	out := []any{}
	for _, arg := range qb.args {
		out = append(out, arg.GetArg())
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
