package processors

import (
	"context"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type AggregatorFunc func(ctx context.Context, key, value, previous interface{}) (newAgg interface{}, err error)

type Aggregator struct {
	AggregatorFunc AggregatorFunc
	Store          string

	topology.DefaultNode
	store stores.Store
}

func (f *Aggregator) ReadsFrom() []string {
	return []string{f.Store}
}

func (f *Aggregator) WritesAt() []string {
	return []string{f.Store}
}

func (f *Aggregator) Init(ctx topology.NodeContext) error {
	f.store = ctx.Store(f.Store)
	return nil
}

func (f *Aggregator) Name() string {
	return `aggregator`
}

func (f *Aggregator) Type() topology.Type {
	return topology.Type{
		Name:  "aggregator",
		Attrs: nil,
	}
}

func (f *Aggregator) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &Aggregator{
		AggregatorFunc: f.AggregatorFunc,
		DefaultNode:    f.DefaultNode,
		Store:          f.Store,
	}, nil
}

func (f *Aggregator) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	// get aggregator value from store
	oldAgg, err := f.store.Get(ctx, kIn)
	if err != nil {
		return nil, nil, false, f.WrapErrWith(err, `old value fetch failed`)
	}

	newAgg, err := f.AggregatorFunc(ctx, kIn, vIn, oldAgg)
	if err != nil {
		return nil, nil, false, f.WrapErrWith(err, `aggregator error`)
	}

	// Tombstone the record
	if newAgg == nil {
		err = f.store.Delete(ctx, kIn)
		if err != nil {
			return nil, nil, false, f.WrapErrWith(err, `aggregator delete failed`)
		}

		return f.Forward(ctx, kIn, newAgg, true)
	}

	err = f.store.Set(ctx, kIn, newAgg, 0)
	if err != nil {
		return nil, nil, false, f.WrapErrWith(err, `aggregator new value set failed`)
	}

	return f.Forward(ctx, kIn, newAgg, true)
}
