package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type FilterFunc func(ctx context.Context, key, value interface{}) (bool, error)

type Filter struct {
	FilterFunc FilterFunc

	topology.DefaultNode
}

func (f *Filter) Name() string {
	return `filter`
}

func (f *Filter) Type() topology.Type {
	return topology.Type{
		Name:  "filter",
		Attrs: nil,
	}
}

func (f *Filter) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &Filter{
		FilterFunc:  f.FilterFunc,
		DefaultNode: f.DefaultNode,
	}, nil
}

func (f *Filter) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	ok, err := f.FilterFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, f.WrapErrWith(err, `filter error`)
	}

	if !ok {
		return f.Ignore()
	}

	return f.Forward(ctx, kIn, vIn, true)
}
