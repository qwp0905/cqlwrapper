package cqlwrapper

import "github.com/pkg/errors"

type queryRecover struct {
	err error
}

func (qb *queryRecover) recover() {
	if err, ok := recover().(error); ok && err != nil {
		qb.err = errors.WithStack(err)
	}
}

func recoverer() *queryRecover {
	return &queryRecover{err: nil}
}
