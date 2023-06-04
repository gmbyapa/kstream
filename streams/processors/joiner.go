package processors

import (
	"context"
)

// JoinerType represents a supported join type eg:  LeftJoin, RightJoin, InnerJoin.
type JoinerType int

func (jt JoinerType) String() string {
	switch jt {
	case LeftJoin:
		return `LeftJoin`
	case RightJoin:
		return `RightJoin`
	case InnerJoin:
		return `InnerJoin`
	case OuterJoin:
		return `OuterJoin`
	}

	return ``
}

const (
	LeftJoin JoinerType = iota
	RightJoin
	InnerJoin
	OuterJoin
)

// Side represents the current side of the join eg: Left, Right.
type Side int

func (jt Side) String() string {
	switch jt {
	case LeftSide:
		return `Left`
	case RightSide:
		return `Right`
	}

	return ``
}

const (
	LeftSide Side = iota
	RightSide
)

type KeyMapper func(ctx context.Context, key, value interface{}) (mappedKey interface{}, err error)

type JoinValueMapper func(ctx context.Context, left, right interface{}) (joined interface{}, err error)
