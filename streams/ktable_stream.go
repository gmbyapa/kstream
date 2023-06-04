package streams

import (
	"github.com/gmbyapa/kstream/streams/processors"
	"github.com/gmbyapa/kstream/streams/topology"
)

type Table interface {
	StreamTopology
	stateStore() topology.LoggableStoreBuilder
	ToStream(opts ...StreamOption) Stream
	Filter(filter processors.FilterFunc, opts ...StreamOption) Table
	Each(eachFunc processors.EachFunc, opts ...StreamOption) Table
	Join(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Table
	JoinGlobalTable(table GlobalTable, keyMapper processors.KeyMapper, valMapper processors.JoinValueMapper, opts ...JoinOption) Table
	LeftJoin(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Table
	RightJoin(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Table
	OuterJoin(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Table
	join(table Table, valMapper processors.JoinValueMapper, typ processors.JoinerType, opts ...JoinOption) Table
	newChildTable(node topology.NodeBuilder, filter processors.FilterFunc, opts ...childStreamOption) *kTableStream
	filters() []processors.FilterFunc
}

type kTableStream struct {
	*kStream
	store       topology.LoggableStoreBuilder
	filterFuncs []processors.FilterFunc
}

func (tbl *kTableStream) newChildTable(node topology.NodeBuilder, filter processors.FilterFunc, opts ...childStreamOption) *kTableStream {
	var filters []processors.FilterFunc
	if filter != nil {
		copy(filters, tbl.filterFuncs)
	}

	return &kTableStream{
		kStream:     tbl.newChildStream(node, opts...),
		store:       tbl.store,
		filterFuncs: filters,
	}
}

func (tbl *kTableStream) stateStore() topology.LoggableStoreBuilder {
	return tbl.store
}

func (tbl *kTableStream) filters() []processors.FilterFunc {
	return tbl.filterFuncs
}

func (tbl *kTableStream) Join(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Table {
	return tbl.join(table, valMapper, processors.InnerJoin, opts...)
}

func (tbl *kTableStream) JoinGlobalTable(table GlobalTable, keyMapper processors.KeyMapper, valMapper processors.JoinValueMapper, opts ...JoinOption) Table {
	return &kTableStream{
		kStream: tbl.kStream.JoinGlobalTable(table, keyMapper, valMapper, opts...).(*kStream),
		store:   tbl.store,
	}
}

func (tbl *kTableStream) LeftJoin(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Table {
	return tbl.join(table, valMapper, processors.LeftJoin, opts...)
}

func (tbl *kTableStream) RightJoin(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Table {
	return tbl.join(table, valMapper, processors.RightJoin, opts...)
}

func (tbl *kTableStream) OuterJoin(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Table {
	return tbl.join(table, valMapper, processors.OuterJoin, opts...)
}

func (tbl *kTableStream) ToStream(opts ...StreamOption) Stream {
	return tbl.newChildStream(tbl.node())
}

func (tbl *kTableStream) Filter(filter processors.FilterFunc, opts ...StreamOption) Table {
	node := &processors.Filter{
		FilterFunc: filter,
	}

	applyNodeOptions(node, opts)
	tbl.topology().AddNodeWithEdge(tbl.node(), node)

	return tbl.newChildTable(node, filter)
}

func (tbl *kTableStream) Each(eachFunc processors.EachFunc, opts ...StreamOption) Table {
	node := &processors.Each{
		EachFunc: eachFunc,
	}

	applyNodeOptions(node, opts)
	tbl.topology().AddNodeWithEdge(tbl.node(), node)

	return tbl.newChildTable(node, nil)
}

func (tbl *kTableStream) join(table Table, valMapper processors.JoinValueMapper, typ processors.JoinerType, opts ...JoinOption) Table {
	// Mark sources for co partitioning
	table.source().ShouldCoPartitionedWith(tbl.source())

	tbl.stpBuilder.AddStore(table.stateStore())

	joinOpts := new(JoinOptions)
	joinOpts.apply(opts...)

	leftJoiner := &processors.StreamJoiner{
		CurrentSide:         processors.LeftSide,
		OtherSideRequired:   typ == processors.RightJoin || typ == processors.InnerJoin,
		OtherStoreName:      table.stateStore().Name(),
		OtherSideFilters:    table.filters(),
		ValueMapper:         valMapper,
		OtherSideLookupFunc: joinOpts.rightLookupFunc,
	}

	rightJoiner := &processors.StreamJoiner{
		CurrentSide:         processors.RightSide,
		OtherSideRequired:   typ == processors.LeftJoin || typ == processors.InnerJoin,
		OtherSideFilters:    tbl.filters(),
		OtherStoreName:      tbl.store.Name(),
		ValueMapper:         valMapper,
		OtherSideLookupFunc: joinOpts.leftLookupFunc,
	}

	// If sub topologies are different merge the other stream to current
	if tbl.topology().Id() != table.topology().Id() {
		tbl.merge(table)
	}

	// Add left and right joiners to the current topology
	tbl.addNode(leftJoiner)
	tbl.topology().AddNodeWithEdge(table.node(), rightJoiner)

	merger := new(processors.Merger)
	applyNodeOptions(merger, joinOpts.streamOptions)

	// Link merger with left and right joiners
	tbl.topology().AddNodeWithEdge(leftJoiner, merger)
	tbl.topology().AddNodeWithEdge(rightJoiner, merger)

	return tbl.newChildTable(merger, nil)
}
