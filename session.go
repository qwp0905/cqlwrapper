package cqlwrapper

import (
	"context"
	"strings"

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

type sort string

const (
	DESC = sort("DESC")
	ASC  = sort("ASC")
)

func questionMarks(len int) string {
	l := []string{}
	for i := 0; i < len; i++ {
		l = append(l, "?")
	}

	return strings.Join(l, ",")
}
