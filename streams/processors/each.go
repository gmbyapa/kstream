package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type EachFunc func(ctx context.Context, key, value interface{})

type Each struct {
	EachFunc EachFunc

	topology.DefaultNode
}

func (f *Each) Name() string {
	return `each`
}

func (f *Each) Type() topology.Type {
	return topology.Type{
		Name:  "for",
		Attrs: nil,
	}
}

func (f *Each) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &Each{
		EachFunc:    f.EachFunc,
		DefaultNode: f.DefaultNode,
	}, nil
}

func (f *Each) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	f.EachFunc(ctx, kIn, vIn)
	return f.Forward(ctx, kIn, vIn, true)
}
