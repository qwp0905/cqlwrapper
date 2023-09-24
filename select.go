package cqlwrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type SelectQueryBuilder struct {
	*queryRecover
	session        *Session
	ctx            context.Context
	allowFiltering bool
	from           string
	fields         []string
	args           []argument
	limit          int
	orderWith      string
	sort           sort
	consistency    gocql.Consistency
}

func newSelectQueryBuilder(ctx context.Context, session *Session) *SelectQueryBuilder {
	return &SelectQueryBuilder{
		queryRecover: recoverer(),
		session:      session,
		consistency:  session.consistency,
		ctx:          ctx,
		fields:       []string{},
		args:         []argument{},
	}
}

func (qb *SelectQueryBuilder) From(a any) *SelectQueryBuilder {
	defer qb.recover()
	if table, ok := a.(string); ok {
		qb.from = table
		return qb
	}

	qb.from = getTableName(a)
	qb.fields = mappingFields(a)

	return qb
}

func (qb *SelectQueryBuilder) Consistency(co gocql.Consistency) *SelectQueryBuilder {
	qb.consistency = co
	return qb
}

func (qb *SelectQueryBuilder) AllowFiltering() *SelectQueryBuilder {
	qb.allowFiltering = true
	return qb
}

func (qb *SelectQueryBuilder) OrderBy(o string, s sort) *SelectQueryBuilder {
	qb.orderWith = o
	qb.sort = s
	return qb
}

func (qb *SelectQueryBuilder) Limit(l int) *SelectQueryBuilder {
	qb.limit = l
	return qb
}

func (qb *SelectQueryBuilder) bind(o op, a any) *SelectQueryBuilder {
	defer qb.recover()
	if qb.from == "" {
		qb = qb.From(a)
	}
	qb.args = mappingArgs(o, a)
	return qb
}

func (qb *SelectQueryBuilder) Where(a any) *SelectQueryBuilder {
	return qb.bind(opEq, a)
}

func (qb *SelectQueryBuilder) WhereGt(a any) *SelectQueryBuilder {
	return qb.bind(opGt, a)
}

func (qb *SelectQueryBuilder) WhereGte(a any) *SelectQueryBuilder {
	return qb.bind(opGte, a)
}

func (qb *SelectQueryBuilder) WhereLt(a any) *SelectQueryBuilder {
	return qb.bind(opLt, a)
}

func (qb *SelectQueryBuilder) WhereLte(a any) *SelectQueryBuilder {
	return qb.bind(opLte, a)
}

func (qb *SelectQueryBuilder) WhereIn(field string, args any) *SelectQueryBuilder {
	qb.args = append(qb.args, argument{
		field:    field,
		operator: opIn,
		arg:      args,
	})
	return qb
}

func (qb *SelectQueryBuilder) getArgs() []any {
	out := []any{}
	for _, arg := range qb.args {
		out = append(out, arg.arg)
	}
	return out
}

func (qb *SelectQueryBuilder) getQuery() string {
	query := fmt.Sprintf(
		`SELECT %s FROM %s`,
		strings.Join(qb.fields, ","),
		qb.from,
	)
	if len(qb.args) > 0 {
		where := []string{}
		for _, arg := range qb.args {
			where = append(where, arg.query())
		}
		query += fmt.Sprintf(" WHERE %s", strings.Join(where, " AND "))
	}
	if qb.orderWith != "" {
		query += fmt.Sprintf(` ORDER BY "%s" %s`, qb.orderWith, qb.sort)
	}
	if qb.limit != 0 {
		query += fmt.Sprintf(` LIMIT %d`, qb.limit)
	}
	if qb.allowFiltering {
		query += " ALLOW FILTERING"
	}

	return query
}

func (qb *SelectQueryBuilder) do() *gocql.Iter {
	return qb.session.
		Query(qb.getQuery()).
		WithContext(qb.ctx).
		Consistency(qb.consistency).
		Bind(qb.getArgs()...).
		Iter()
}

func (qb *SelectQueryBuilder) First(s any) error {
	if qb.err != nil {
		return qb.err
	}

	qb = qb.Limit(1)
	if len(qb.fields) == 0 {
		qb = qb.From(s)
	}

	iter := qb.do()
	defer iter.Close()
	iter.Scanner()

	rd, err := iter.RowData()
	if err != nil {
		return errors.WithStack(err)
	}
	if !iter.Scan(rd.Values...) {
		return errors.WithStack(gocql.ErrNotFound)
	}

	return assignValues(s, rd.Values)
}

func (qb *SelectQueryBuilder) Count() (int, error) {
	if qb.err != nil {
		return 0, qb.err
	}
	iter := qb.do()
	count := iter.NumRows()
	if err := iter.Close(); err != nil {
		return 0, errors.WithStack(err)
	}
	return count, nil
}

func (qb *SelectQueryBuilder) Scanner() *Scanner {
	if qb.err != nil {
		return &Scanner{iter: nil, err: qb.err}
	}
	iter := qb.do()
	return &Scanner{iter: iter, rows: []any{}, err: nil}
}

type Scanner struct {
	rows []any
	iter *gocql.Iter
	err  error
}

func (s *Scanner) Next() bool {
	if s.err != nil {
		return false
	}
	rd, err := s.iter.RowData()
	if err != nil {
		s.err = errors.WithStack(err)
		return false
	}
	if !s.iter.Scan(rd.Values...) {
		return false
	}
	s.rows = rd.Values
	return true
}

func (s *Scanner) Scan(a any) error {
	if s.err != nil {
		return s.err
	}

	return assignValues(a, s.rows)
}

func (s *Scanner) Err() error {
	return s.iter.Close()
}
