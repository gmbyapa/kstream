package streams

import (
	"context"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/processors"
	"github.com/gmbyapa/kstream/streams/state_stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type StreamTopology interface {
	topology() topology.SubTopologyBuilder
	setSubTopology(topology topology.SubTopologyBuilder)
	node() topology.NodeBuilder
	source() topology.Source
	merge(stream StreamTopology)
}

type GlobalTable interface {
	StreamTopology
	Store() topology.LoggableStoreBuilder
	Stream
}

type SinkOption interface{}

type KeyMapper func(ctx context.Context, key, value interface{}) (mappedKey interface{}, err error)

type ValueMapper func(ctx context.Context, left, right interface{}) (joined interface{}, err error)

type JoinType int

const (
	LeftJoin JoinType = iota
	InnerJoin
)

type Branch struct {
	name   string
	stream Stream
}

type BranchedStream struct {
	parentStream Stream
	parent       *processors.Branch
	branches     []Branch
}

func (brs *BranchedStream) New(name string, predicate processors.BranchPredicate) Stream {
	childBranch := &processors.BranchChild{
		Name:      name,
		Predicate: predicate,
	}

	brs.parentStream.topology().AddNodeWithEdge(brs.parent, childBranch)
	childStream := brs.parentStream.newChildStream(childBranch)

	brs.branches = append(brs.branches, Branch{
		name:   name,
		stream: childStream,
	})

	return childStream
}

func (brs *BranchedStream) Branch(name string) Stream {
	for _, br := range brs.branches {
		if br.name == name {
			return br.stream
		}
	}

	return nil
}

type Stream interface {
	Branch(branches ...processors.BranchDetails) []Stream
	Split(opts ...StreamOption) *BranchedStream
	SelectKey(selectKeyFunc processors.SelectKeyFunc, opts ...StreamOption) Stream
	Aggregate(store string, aggregatorFunc processors.AggregatorFunc, opts ...AggregateOpt) Table
	aggregate(store string, aggregatorFunc processors.AggregatorFunc, optApplyier *AggregateOpts) Table
	FlatMap(flatMapFunc processors.FlatMapFunc, opts ...StreamOption) Stream
	FlatMapValues(flatMapFunc processors.FlatMapValuesFunc, opts ...StreamOption) Stream
	Map(mapper processors.MapperFunc, opts ...StreamOption) Stream
	MapValue(valTransformFunc processors.MapValueFunc, opts ...StreamOption) Stream
	Filter(filter processors.FilterFunc, opts ...StreamOption) Stream
	Each(eachFunc processors.EachFunc, opts ...StreamOption) Stream
	NewProcessor(node topology.NodeBuilder, opts ...StreamOption) Stream
	JoinGlobalTable(table GlobalTable, keyMapper processors.KeyMapper, valMapper processors.JoinValueMapper, opts ...JoinOption) Stream
	LeftJoinGlobalTable(table GlobalTable, keyMapper processors.KeyMapper, valMapper processors.JoinValueMapper, opts ...JoinOption) Stream
	JoinTable(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Stream
	LeftJoinTable(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Stream
	joinTable(table Table, valMapper processors.JoinValueMapper, typ processors.JoinerType, opts ...JoinOption) Stream
	// Through redirect the stream through an intermediate topic
	// Deprecated: use Repartition instead
	Through(topic string, options ...DslOption) Stream
	ToTable(store string, options ...TableOpt) Table
	Merge(stream Stream) Stream
	Repartition(topic string, opts ...RepartitionOpt) Stream
	AddStateStore(name string, keyEnc, valEnc encoding.Encoder, options ...state_stores.StoreBuilderOption)
	To(topic string, options ...KSinkOption)
	StreamTopology
	newChildStream(node topology.NodeBuilder, opts ...childStreamOption) *kStream
	keyEncoder() encoding.Encoder
	valEncoder() encoding.Encoder
}
