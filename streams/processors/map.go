package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type MapperFunc func(ctx context.Context, key, value interface{}) (kOut, vOut interface{}, err error)

type Map struct {
	MapperFunc MapperFunc
	topology.DefaultNode
}

func (t *Map) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &Map{
		MapperFunc:  t.MapperFunc,
		DefaultNode: t.DefaultNode,
	}, nil
}

func (t *Map) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	kTransformed, vTransformed, err := t.MapperFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, t.WrapErrWith(err, `transformer error`)
	}

	return t.Forward(ctx, kTransformed, vTransformed, true)
}

func (t *Map) Type() topology.Type {
	return topology.Type{
		Name:  `map`,
		Attrs: nil,
	}
}

func (t *Map) Name() string {
	return `Map`
}
