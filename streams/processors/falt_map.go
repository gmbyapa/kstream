package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type FlatMapFunc func(ctx context.Context, key, value interface{}) ([]topology.KeyValPair, error)

type FlatMap struct {
	FlatMapFunc FlatMapFunc

	topology.DefaultNode
}

func (f *FlatMap) Name() string {
	return `flatmap`
}

func (f *FlatMap) Type() topology.Type {
	return topology.Type{
		Name:  "flatmap",
		Attrs: nil,
	}
}

func (f *FlatMap) Build(_ topology.SubTopologyContext) (topology.Node, error) {
	return &FlatMap{
		FlatMapFunc: f.FlatMapFunc,
		DefaultNode: f.DefaultNode,
	}, nil
}

func (f *FlatMap) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	kvs, err := f.FlatMapFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, f.WrapErrWith(err, `FlatMapFunc error`)
	}

	for _, kv := range kvs {
		_, _, next, err = f.Forward(ctx, kv.Key, kv.Value, true)
		if err != nil {
			return nil, nil, false, f.WrapErr(err)
		}
	}

	return
}
