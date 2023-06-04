package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type FlatMapValuesFunc func(ctx context.Context, key, value interface{}) ([]interface{}, error)

type FlatMapValues struct {
	FlatMapValuesFunc FlatMapValuesFunc

	topology.DefaultNode
}

func (f *FlatMapValues) Name() string {
	return `flatmap`
}

func (f *FlatMapValues) Type() topology.Type {
	return topology.Type{
		Name:  "flatmap_values",
		Attrs: nil,
	}
}

func (f *FlatMapValues) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &FlatMapValues{
		FlatMapValuesFunc: f.FlatMapValuesFunc,
		DefaultNode:       f.DefaultNode,
	}, nil
}

func (f *FlatMapValues) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	values, err := f.FlatMapValuesFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, f.WrapErrWith(err, `FlatMapValues error`)
	}

	for _, val := range values {
		_, _, next, err = f.Forward(ctx, kIn, val, true)
		if err != nil {
			return nil, nil, false, f.WrapErrWith(err, `FlatMapValues error`)
		}
	}

	return
}
