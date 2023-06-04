package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type SelectKeyFunc func(ctx context.Context, key, value interface{}) (kOut interface{}, err error)

type KeySelector struct {
	SelectKeyFunc SelectKeyFunc

	topology.DefaultNode
}

func (ks *KeySelector) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &KeySelector{
		SelectKeyFunc: ks.SelectKeyFunc,
		DefaultNode:   ks.DefaultNode,
	}, nil
}

func (ks *KeySelector) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	k, err := ks.SelectKeyFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, ks.WrapErrWith(err, `error in select key function`)
	}

	return ks.Forward(ctx, k, vIn, true)
}

func (ks *KeySelector) Type() topology.Type {
	return topology.Type{
		Name:  `key_selector`,
		Attrs: nil,
	}
}
