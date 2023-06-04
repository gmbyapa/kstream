package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type MapValueFunc func(ctx context.Context, key, value interface{}) (vOut interface{}, err error)

type ValueMapper struct {
	MapValueFunc MapValueFunc
	topology.DefaultNode
}

func (vt *ValueMapper) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &ValueMapper{
		MapValueFunc: vt.MapValueFunc,
		DefaultNode:  vt.DefaultNode,
	}, nil
}

func (vt *ValueMapper) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	transformed, err := vt.MapValueFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, vt.WrapErrWith(err, `error in value transform function`)
	}

	return vt.Forward(ctx, kIn, transformed, true)
}

func (vt *ValueMapper) Type() topology.Type {
	return topology.Type{
		Name:  `value_mapper`,
		Attrs: nil,
	}
}
