package cqlwrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/qwp0905/cqlwrapper/internal/mapper"
)

type DeleteQueryBuilder struct {
	*queryRecover
	session     *Session
	ctx         context.Context
	consistency gocql.Consistency
	args        []*mapper.Argument
	model       *mapper.Mapper
}

func newDeleteQueryBuilder(ctx context.Context, session *Session) *DeleteQueryBuilder {
	return &DeleteQueryBuilder{
		queryRecover: recoverer(),
		session:      session,
		consistency:  session.consistency,
		ctx:          ctx,
		args:         []*mapper.Argument{},
		model:        nil,
	}
}

func (qb *DeleteQueryBuilder) From(a any) *DeleteQueryBuilder {
	qb.model = mapper.New(a)
	return qb
}

func (qb *DeleteQueryBuilder) Consistency(co gocql.Consistency) *DeleteQueryBuilder {
	defer qb.recover()
	qb.consistency = co
	return qb
}

func (qb *DeleteQueryBuilder) Where(a any) *DeleteQueryBuilder {
	defer qb.recover()
	if qb.model == nil {
		qb = qb.From(a)
	}
	qb.args = qb.model.MappingArgs(mapper.OpEq, a)

	return qb
}

func (qb *DeleteQueryBuilder) getArgs() []any {
	out := []any{}
	for _, arg := range qb.args {
		out = append(out, arg.GetArg())
	}
	return out
}

func (qb *DeleteQueryBuilder) getQuery() string {
	where := []string{}
	for _, arg := range qb.args {
		where = append(where, arg.Query())
	}
	return fmt.Sprintf(
		`DELETE FROM %s WHERE %s`,
		qb.model.GetTable(),
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
