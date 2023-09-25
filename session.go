package cqlwrapper

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type Session struct {
	*gocql.Session
	consistency gocql.Consistency
}

func New(cluster *gocql.ClusterConfig) (*Session, error) {
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &Session{
		Session:     session,
		consistency: cluster.Consistency,
	}, nil
}

func (s *Session) SelectWithContext(ctx context.Context) *SelectQueryBuilder {
	return newSelectQueryBuilder(ctx, s)
}

func (s *Session) InsertWithContext(ctx context.Context) *InsertQueryBuilder {
	return newInsertQueryBuilder(ctx, s)
}

func (s *Session) UpdateWithContext(ctx context.Context) *UpdateQueryBuilder {
	return newUpdateQueryBuilder(ctx, s)
}

func (s *Session) DeleteWithContext(ctx context.Context) *DeleteQueryBuilder {
	return newDeleteQueryBuilder(ctx, s)
}

type argument struct {
	field    string
	operator op
	arg      any
}

func (a *argument) query() string {
	return fmt.Sprintf(`"%s" %s ?`, a.field, a.operator)
}

func (a *argument) getField() string {
	return fmt.Sprintf(`"%s"`, a.field)
}

type op string

const (
	opEq  = op("=")
	opGt  = op(">")
	opGte = op(">=")
	opLt  = op("<")
	opLte = op("<=")
	opIn  = op("IN")
)

type sort string

const (
	DESC = sort("DESC")
	ASC  = sort("ASC")
)
