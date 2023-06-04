package processors

import (
	"context"
	"github.com/gmbyapa/kstream/pkg/errors"

	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type StreamJoinerOption func(joiner *StreamJoiner)

type ValueLookupFunc func(ctx context.Context,
	store stores.ReadOnlyStore, key, leftVal interface{}) (interface{}, error)

var defaultLookupFunc ValueLookupFunc = func(ctx context.Context,
	store stores.ReadOnlyStore, key, val interface{}) (interface{}, error) {
	v, err := store.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, `value lockup failed`)
	}

	return v, nil
}

type StreamJoiner struct {
	OtherSideRequired   bool
	CurrentSide         Side
	OtherStoreName      string
	ValueMapper         JoinValueMapper
	OtherSideLookupFunc ValueLookupFunc
	OtherSideFilters    []FilterFunc

	otherStore stores.Store
	topology.DefaultNode
}

func (sj *StreamJoiner) ReadsFrom() []string {
	return []string{sj.OtherStoreName}
}

func (sj *StreamJoiner) Init(ctx topology.NodeContext) error {
	sj.otherStore = ctx.Store(sj.OtherStoreName)

	return nil
}

func (sj *StreamJoiner) Build(_ topology.SubTopologyContext) (topology.Node, error) {
	lookupFunc := defaultLookupFunc
	if sj.OtherSideLookupFunc != nil {
		lookupFunc = sj.OtherSideLookupFunc
	}

	return &StreamJoiner{
		OtherSideRequired:   sj.OtherSideRequired,
		CurrentSide:         sj.CurrentSide,
		OtherStoreName:      sj.OtherStoreName,
		ValueMapper:         sj.ValueMapper,
		OtherSideLookupFunc: lookupFunc,
		OtherSideFilters:    sj.OtherSideFilters,

		DefaultNode: sj.DefaultNode,
	}, nil
}

func (sj *StreamJoiner) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	// Get the other side value/values
	value, err := sj.OtherSideLookupFunc(ctx, sj.otherStore, kIn, vIn)
	if err != nil {
		return sj.IgnoreAndWrapErrWith(err, `RightTable read failed`)
	}

	// other side value is empty. no need to evaluate filters
	if value == nil {
		goto CONT
	}

	// Apply filters
	for _, f := range sj.OtherSideFilters {
		ok, err := f(ctx, kIn, value)
		if err != nil {
			return sj.IgnoreAndWrapErrWith(err, `OtherSideFilters filter func failed`)
		}

		// if any filter returns false, stop evaluating others and make value nil
		// (so it will be ignored accordingly, eg: inner join)
		if !ok {
			value = nil
			break
		}
	}

	// If other side is required (eg: inner join, or the left side of a left join) then just ignore
CONT:
	if value == nil && sj.OtherSideRequired {
		return sj.Ignore()
	}

	var l, r interface{}
	switch sj.CurrentSide {
	case LeftSide:
		l, r = vIn, value
	case RightSide:
		l, r = value, vIn
	}

	joinedValue, err := sj.ValueMapper(ctx, l, r)
	if err != nil {
		return sj.IgnoreAndWrapErrWith(err, `value mapper failed`)
	}

	return sj.Forward(ctx, kIn, joinedValue, true)
}

func (sj *StreamJoiner) Name() string {
	return `joiner`
}

func (sj *StreamJoiner) Type() topology.Type {
	return topology.Type{
		Name: "joiner",
		Attrs: map[string]string{
			`side`: sj.CurrentSide.String(),
		},
	}
}
