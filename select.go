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

func (qb *SelectQueryBuilder) Columns(fields ...string) *SelectQueryBuilder {
	qb.fields = fields
	return qb
}

func (qb *SelectQueryBuilder) From(a any) *SelectQueryBuilder {
	defer qb.recover()
	if table, ok := a.(string); ok {
		qb.from = table
		return qb
	}

	qb.from = getTableName(a)

	if len(qb.fields) == 0 {
		qb.fields = mappingFields(a)
	}

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
	if !qb.isPrepared() {
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

func (qb *SelectQueryBuilder) getFields() string {
	out := []string{}
	for _, field := range qb.fields {
		out = append(out, fmt.Sprintf(`"%s"`, field))
	}
	return strings.Join(out, ",")
}

func (qb *SelectQueryBuilder) getArgs() []any {
	out := []any{}
	for _, arg := range qb.args {
		out = append(out, arg.arg)
	}
	return out
}

func (qb *SelectQueryBuilder) getQuery() string {
	query := fmt.Sprintf(`SELECT %s FROM %s`, qb.getFields(), qb.from)
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

func (qb *SelectQueryBuilder) isPrepared() bool {
	return qb.from != "" && len(qb.fields) != 0
}

func (qb *SelectQueryBuilder) iter() *gocql.Iter {
	return qb.session.
		Query(qb.getQuery()).
		WithContext(qb.ctx).
		Consistency(qb.consistency).
		Bind(qb.getArgs()...).
		Iter()
}

func (qb *SelectQueryBuilder) First(s any) (err error) {
	defer func() {
		if e, ok := recover().(error); ok && e != nil {
			err = e
			return
		}
	}()
	if err = qb.error(); err != nil {
		return
	}

	if !qb.isPrepared() {
		qb = qb.From(s)
	}

	iter := qb.Limit(1).iter()
	defer iter.Close()

	var rd gocql.RowData
	rd, err = iter.RowData()
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	if !iter.Scan(rd.Values...) {
		err = errors.WithStack(gocql.ErrNotFound)
		return
	}

	return assignValues(s, qb.fields, rd.Values)
}

func (qb *SelectQueryBuilder) All(s any) (err error) {
	defer func() {
		if e, ok := recover().(error); ok && e != nil {
			err = e
			return
		}
	}()
	if err = qb.error(); err != nil {
		return
	}

	if !qb.isPrepared() {
		qb = qb.From(s)
	}

	iter := qb.iter()
	defer iter.Close()

	for {
		var rd gocql.RowData
		rd, err = iter.RowData()
		if err != nil {
			return errors.WithStack(err)
		}
		if !iter.Scan(rd.Values...) {
			break
		}
		if err = appendValues(s, qb.fields, rd.Values); err != nil {
			return
		}
	}

	return
}

func (qb *SelectQueryBuilder) Count() (int, error) {
	if err := qb.error(); err != nil {
		return 0, err
	}
	iter := qb.iter()
	defer iter.Close()
	return iter.NumRows(), nil
}

func (qb *SelectQueryBuilder) Scanner() *Scanner {
	if err := qb.error(); err != nil {
		return &Scanner{iter: nil, err: err}
	}
	iter := qb.iter()
	return &Scanner{iter: iter, fields: qb.fields, rows: []any{}, err: nil}
}

type Scanner struct {
	rows   []any
	fields []string
	iter   *gocql.Iter
	err    error
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

func (s *Scanner) Scan(a any) (err error) {
	defer func() {
		if e, ok := recover().(error); ok && e != nil {
			err = e
			return
		}
	}()
	if s.err != nil {
		return s.err
	}

	return assignValues(a, s.fields, s.rows)
}

func (s *Scanner) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.iter.Close()
}
