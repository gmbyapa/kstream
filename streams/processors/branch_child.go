package processors

import (
	"context"
	"fmt"

	"github.com/gmbyapa/kstream/streams/topology"
)

type BranchPredicate func(ctx context.Context, key interface{}, val interface{}) (bool, error)

type BranchDetails struct {
	Name      string
	Predicate BranchPredicate
}

type BranchChild struct {
	Name      string
	Predicate BranchPredicate

	topology.DefaultNode
}

func (b *BranchChild) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &BranchChild{
		Name:        b.Name,
		Predicate:   b.Predicate,
		DefaultNode: b.DefaultNode,
	}, nil
}

func (b *BranchChild) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	return b.Forward(ctx, kIn, vIn, true)
}

func (b *BranchChild) Type() topology.Type {
	return topology.Type{
		Name: `branch`,
		Attrs: map[string]string{
			`name`: fmt.Sprintf(`Branch (%s)`, b.Name),
		},
	}
}
