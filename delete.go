package cqlwrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type DeleteQueryBuilder struct {
	*queryRecover
	session     *Session
	ctx         context.Context
	consistency gocql.Consistency
	args        []argument
	from        string
}

func newDeleteQueryBuilder(ctx context.Context, session *Session) *DeleteQueryBuilder {
	return &DeleteQueryBuilder{
		queryRecover: recoverer(),
		session:      session,
		consistency:  session.consistency,
		ctx:          ctx,
		args:         []argument{},
	}
}

func (qb *DeleteQueryBuilder) From(a any) *DeleteQueryBuilder {
	if table, ok := a.(string); ok {
		qb.from = table
		return qb
	}

	qb.from = getTableName(a)
	return qb
}

func (qb *DeleteQueryBuilder) Consistency(co gocql.Consistency) *DeleteQueryBuilder {
	qb.consistency = co
	return qb
}

func (qb *DeleteQueryBuilder) Where(a any) *DeleteQueryBuilder {
	defer qb.recover()
	if qb.from == "" {
		qb.from = getTableName(a)
	}
	qb.args = mappingArgs(opEq, a)

	return qb
}

func (qb *DeleteQueryBuilder) getArgs() []any {
	out := []any{}
	for _, arg := range qb.args {
		out = append(out, arg.arg)
	}
	return out
}

func (qb *DeleteQueryBuilder) getQuery() string {
	where := []string{}
	for _, arg := range qb.args {
		where = append(where, arg.query())
	}
	return fmt.Sprintf(
		`DELETE FROM %s WHERE %s`,
		qb.from,
		strings.Join(where, " AND "),
	)
}

func (qb *DeleteQueryBuilder) Exec() error {
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
