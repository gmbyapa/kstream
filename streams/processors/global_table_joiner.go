package processors

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type GlobalTableJoiner struct {
	Store              string
	KeyMapper          KeyMapper
	ValueMapper        JoinValueMapper
	JoinType           JoinerType
	RightKeyLookupFunc ValueLookupFunc

	store stores.ReadOnlyStore
	topology.DefaultNode
}

func (j *GlobalTableJoiner) Type() topology.Type {
	return topology.Type{
		Name: "GTableJoiner",
		Attrs: map[string]string{
			`store`: j.Store,
		},
	}
}

func (j *GlobalTableJoiner) ReadsFrom() []string {
	return []string{j.Store}
}

func (j *GlobalTableJoiner) StateType() topology.StateType {
	return topology.StateReadOnly
}

func (j *GlobalTableJoiner) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	joined, err := j.Join(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, j.WrapErr(err)
	}

	return j.Forward(ctx, kIn, joined, true)
}

func (j *GlobalTableJoiner) Build(_ topology.SubTopologyContext) (topology.Node, error) {
	lookupFunc := func(ctx context.Context, store stores.ReadOnlyStore, key, val interface{}) (interface{}, error) {
		v, err := store.Get(ctx, key)
		if err != nil {
			return nil, errors.Wrap(err, `value loocup failed`)
		}

		return v, nil
	}

	if j.RightKeyLookupFunc != nil {
		lookupFunc = j.RightKeyLookupFunc
	}

	return &GlobalTableJoiner{
		KeyMapper:          j.KeyMapper,
		ValueMapper:        j.ValueMapper,
		JoinType:           j.JoinType,
		Store:              j.Store,
		DefaultNode:        j.DefaultNode,
		RightKeyLookupFunc: lookupFunc,
	}, nil
}

func (j *GlobalTableJoiner) Init(ctx topology.NodeContext) error {
	stor, err := ctx.StoreRegistry().Store(j.Store)
	if err != nil {
		return errors.Wrap(err, `GlobalTable Init failed`)
	}
	j.store = stor
	return nil
}

func (j *GlobalTableJoiner) Join(ctx context.Context, key, leftVal interface{}) (joinedVal interface{}, err error) {
	// get key from key mapper
	k, err := j.KeyMapper(ctx, key, leftVal)
	if err != nil {
		return nil, j.WrapErrWith(err, `KeyMapper error`)
	}

	// get value from store
	rightValue, err := j.RightKeyLookupFunc(ctx, j.store, k, leftVal)
	if err != nil {
		return nil, j.WrapErrWith(err,
			fmt.Sprintf(`cannot get value from [%s] store`, j.store))
	}

	// For InnerJoin joins if the right side lookup returns nil ignore the join
	if j.JoinType == InnerJoin && rightValue == nil {
		return nil, j.WrapErr(errors.New(
			fmt.Sprintf(`right value lookup failed due to key [%+v] dose not exist in %s store]`, k, j.store.Name())))
	}

	// Send Left value and Right value to ValueMapper and get the joined value
	valJoined, err := j.ValueMapper(ctx, leftVal, rightValue)
	if err != nil {
		return nil, j.WrapErrWith(err, `value mapper failed`)
	}

	return valJoined, nil
}
