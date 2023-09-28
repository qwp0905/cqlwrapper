package cqlwrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/qwp0905/cqlwrapper/internal/mapper"
)

type SelectQueryBuilder struct {
	*queryRecover
	session        *Session
	ctx            context.Context
	allowFiltering bool
	fields         []string
	args           []*mapper.Argument
	limit          int
	orderWith      string
	sort           sort
	consistency    gocql.Consistency
	model          *mapper.Mapper
}

func newSelectQueryBuilder(ctx context.Context, session *Session) *SelectQueryBuilder {
	return &SelectQueryBuilder{
		queryRecover: recoverer(),
		session:      session,
		consistency:  session.consistency,
		ctx:          ctx,
		fields:       []string{},
		args:         []*mapper.Argument{},
		model:        nil,
	}
}

func (qb *SelectQueryBuilder) Columns(fields ...string) *SelectQueryBuilder {
	qb.fields = fields
	return qb
}

func (qb *SelectQueryBuilder) From(a any) *SelectQueryBuilder {
	defer qb.recover()
	qb.model = mapper.New(a)

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

func (qb *SelectQueryBuilder) bind(o mapper.Operator, a any) *SelectQueryBuilder {
	defer qb.recover()
	if !qb.isPrepared() {
		qb = qb.From(a)
	}

	qb.args = append(qb.args, qb.model.MappingArgs(o, a)...)
	return qb
}

func (qb *SelectQueryBuilder) Where(a any) *SelectQueryBuilder {
	return qb.bind(mapper.OpEq, a)
}

func (qb *SelectQueryBuilder) WhereGt(a any) *SelectQueryBuilder {
	return qb.bind(mapper.OpGt, a)
}

func (qb *SelectQueryBuilder) WhereGte(a any) *SelectQueryBuilder {
	return qb.bind(mapper.OpGte, a)
}

func (qb *SelectQueryBuilder) WhereLt(a any) *SelectQueryBuilder {
	return qb.bind(mapper.OpLt, a)
}

func (qb *SelectQueryBuilder) WhereLte(a any) *SelectQueryBuilder {
	return qb.bind(mapper.OpLte, a)
}

func (qb *SelectQueryBuilder) WhereIn(field string, args any) *SelectQueryBuilder {
	qb.args = append(qb.args, mapper.NewArg(field, mapper.OpIn, args))
	return qb
}

func (qb *SelectQueryBuilder) getFields() string {
	if len(qb.fields) == 0 {
		qb.fields = qb.model.GetFields()
	}

	out := []string{}
	for _, field := range qb.fields {
		out = append(out, fmt.Sprintf(`"%s"`, field))
	}
	return strings.Join(out, ",")
}

func (qb *SelectQueryBuilder) getArgs() []any {
	out := []any{}
	for _, arg := range qb.args {
		out = append(out, arg.GetArg())
	}
	return out
}

func (qb *SelectQueryBuilder) getQuery() string {
	query := fmt.Sprintf(`SELECT %s FROM %s`, qb.getFields(), qb.model.GetTable())
	if len(qb.args) > 0 {
		where := []string{}
		for _, arg := range qb.args {
			where = append(where, arg.Query())
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
	return qb.model != nil
}

func (qb *SelectQueryBuilder) iter() *gocql.Iter {
	return qb.session.
		Query(qb.getQuery()).
		WithContext(qb.ctx).
		Consistency(qb.consistency).
		Bind(qb.getArgs()...).
		Iter()
}

func (qb *SelectQueryBuilder) One(s any) (err error) {
	if !qb.isPrepared() {
		qb = qb.From(s)
	}

	if err = qb.error(); err != nil {
		return err
	}

	iter := qb.Limit(1).iter()
	defer iter.Close()

	var rd gocql.RowData
	rd, err = iter.RowData()
	if err != nil {
		return errors.WithStack(err)
	}
	if !iter.Scan(rd.Values...) {
		return errors.WithStack(gocql.ErrNotFound)
	}

	return qb.model.AssignValues(s, qb.fields, rd.Values)
}

func (qb *SelectQueryBuilder) All(s any) (err error) {
	if !qb.isPrepared() {
		qb = qb.From(s)
	}

	if err = qb.error(); err != nil {
		return err
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
		if err = qb.model.AppendValues(s, qb.fields, rd.Values); err != nil {
			return err
		}
	}

	return nil
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
	if qb.model == nil {
		return &Scanner{err: errors.New("model not defined")}
	}

	return &Scanner{
		iter:   qb.iter(),
		fields: qb.fields,
		model:  qb.model,
		rows:   []any{},
		err:    nil,
	}
}

type Scanner struct {
	rows   []any
	fields []string
	iter   *gocql.Iter
	err    error
	model  *mapper.Mapper
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
	if s.err != nil {
		return s.err
	}

	return s.model.AssignValues(a, s.fields, s.rows)
}

func (s *Scanner) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.iter.Close()
}
