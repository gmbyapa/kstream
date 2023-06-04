package processors

import (
	"context"
	"time"

	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type WindowJoinerOption func(joiner *WindowJoiner)

type WindowJoiner struct {
	OtherSideRequired         bool
	CurrentSide               Side
	Duration                  time.Duration
	StoreName, OtherStoreName string
	ValueMapper               JoinValueMapper

	window      stores.Store
	otherWindow stores.Store
	topology.DefaultNode
}

func (sj *WindowJoiner) ReadsFrom() []string {
	return []string{sj.OtherStoreName}
}

func (sj *WindowJoiner) WritesAt() []string {
	return []string{sj.StoreName}
}

func (sj *WindowJoiner) Init(ctx topology.NodeContext) error {
	sj.window = ctx.Store(sj.StoreName)
	sj.otherWindow = ctx.Store(sj.OtherStoreName)

	return nil
}

func (sj *WindowJoiner) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &WindowJoiner{
		OtherSideRequired: sj.OtherSideRequired,
		CurrentSide:       sj.CurrentSide,
		Duration:          sj.Duration,
		StoreName:         sj.StoreName,
		OtherStoreName:    sj.OtherStoreName,
		ValueMapper:       sj.ValueMapper,
		DefaultNode:       sj.DefaultNode,
	}, nil
}

func (sj *WindowJoiner) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	var joinedValue interface{}

	otherValue, err := sj.otherWindow.Get(ctx, kIn)
	if err != nil {
		return nil, nil, false, sj.WrapErrWith(err, `RightWindow read failed`)
	}

	// If other side is required then just write the value and ignore
	if otherValue == nil && sj.OtherSideRequired {
		if err := sj.window.Set(ctx, kIn, vIn, sj.Duration); err != nil {
			return nil, nil, false, sj.WrapErrWith(err, `LeftWindow write failed`)
		}

		return sj.Ignore()
	}

	var l, r interface{}
	switch sj.CurrentSide {
	case LeftSide:
		l = vIn
		r = otherValue
	case RightSide:
		l = otherValue
		r = vIn
	}

	joinedValue, err = sj.ValueMapper(ctx, l, r)
	if err != nil {
		return nil, nil, false, sj.WrapErrWith(err, `value mapper failed`)
	}

	return sj.Forward(ctx, kIn, joinedValue, true)
}

func (sj *WindowJoiner) Name() string {
	return `joiner`
}

func (sj *WindowJoiner) Type() topology.Type {
	return topology.Type{
		Name:  "joiner",
		Attrs: nil,
	}
}
