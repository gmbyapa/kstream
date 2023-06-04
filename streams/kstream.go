package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/processors"
	"github.com/gmbyapa/kstream/streams/state_stores"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type TopicNameFormatter func(topic string) func(ctx topology.BuilderContext, nodeId topology.NodeId) string

type DslOptions struct {
	StreamOptions
	kSinkOptions   []KSinkOption
	kSourceOptions []KSourceOption
}

type DslOption func(options *DslOptions)

func (dOpts *DslOptions) apply(opts ...DslOption) {
	for _, opt := range opts {
		opt(dOpts)
	}
}

func DslWithSinkOptions(opts ...KSinkOption) DslOption {
	return func(options *DslOptions) {
		options.kSinkOptions = append(options.kSinkOptions, opts...)
	}
}

func DslOptsSinkOptions(opts ...KSinkOption) DslOption {
	return func(options *DslOptions) {
		options.kSinkOptions = append(options.kSinkOptions, opts...)
	}
}

func DslOptsSourceOptions(opts ...KSourceOption) DslOption {
	return func(options *DslOptions) {
		options.kSourceOptions = append(options.kSourceOptions, opts...)
	}
}

type rePartitioned struct {
	must  []RepartitionOpt
	mayBe []RepartitionOpt
}

func (rp *rePartitioned) Apply() []RepartitionOpt {
	if rp.mayBe == nil {
		return nil
	}
	return append(rp.mayBe, rp.must...)
}

type childStreamOption func(stream *kStream)

func childStreamWithKeyEncoder(encoder encoding.Encoder) childStreamOption {
	return func(stream *kStream) {
		stream.encoders.key = encoder
	}
}

func childStreamWithValEncoder(encoder encoding.Encoder) childStreamOption {
	return func(stream *kStream) {
		stream.encoders.val = encoder
	}
}

type kStream struct {
	tpBuilder       topology.Builder
	stpBuilder      topology.SubTopologyBuilder
	kSource         topology.Source
	rootNode        topology.NodeBuilder
	builder         *StreamBuilder
	rePartitioned   bool
	repartitionOpts *rePartitioned
	encoders        struct {
		key, val encoding.Encoder
	}
}

func (k *kStream) newChildStream(node topology.NodeBuilder, opts ...childStreamOption) *kStream {
	child := &kStream{
		tpBuilder:       k.tpBuilder,
		stpBuilder:      k.stpBuilder,
		rootNode:        node,
		kSource:         k.kSource,
		builder:         k.builder,
		repartitionOpts: k.repartitionOpts,
		rePartitioned:   k.rePartitioned,
	}
	child.encoders.key = k.keyEncoder()
	child.encoders.val = k.valEncoder()

	for _, opt := range opts {
		opt(child)
	}

	return child
}

func (k *kStream) maybeRepartitioned(opts ...RepartitionOpt) *kStream {
	// TODO fix this
	// TODO need to fix this logic as it will ignore if this stream is already repartitioned
	if k.repartitionOpts == nil {
		k.repartitionOpts = &rePartitioned{}
	}
	k.repartitionOpts.mayBe = append(k.repartitionOpts.mayBe, rePartitionedWithSourceOpts(
		ConsumeWithAutoTopicCreateEnabled(
			WithReplicaCount(k.builder.config.InternalTopicsDefaultReplicaCount),
		)))
	k.repartitionOpts.mayBe = append(k.repartitionOpts.mayBe, opts...)
	return k
}

func (k *kStream) Branch(branches ...processors.BranchDetails) []Stream {
	// create a branch node
	br := new(processors.Branch)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, br)

	var childStreams []Stream

	for _, child := range branches {
		childNode := &processors.BranchChild{
			Name:      child.Name,
			Predicate: child.Predicate,
		}
		k.stpBuilder.AddNodeWithEdge(br, childNode)
		childStreams = append(childStreams, k.newChildStream(childNode))
	}

	return childStreams
}

func (k *kStream) Split(opts ...StreamOption) *BranchedStream {
	node := &processors.Branch{}
	applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	return &BranchedStream{parentStream: k, parent: node}
}

func (k *kStream) NewProcessor(node topology.NodeBuilder, opts ...StreamOption) Stream {
	applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	return k.newChildStream(node)
}

type Transformer func(ctx topology.NodeContext, key, value interface{})

func (k *kStream) TransformValues(node topology.NodeBuilder) Stream {
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	return k.newChildStream(node)
}

func (k *kStream) MapValue(mapperFunc processors.MapValueFunc, opts ...StreamOption) Stream {
	node := &processors.ValueMapper{
		MapValueFunc: mapperFunc,
	}
	applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	return k.newChildStream(node)
}

func (k *kStream) Map(transformer processors.MapperFunc, opts ...StreamOption) Stream {
	node := &processors.Map{
		MapperFunc: transformer,
	}
	optsApplied := applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	if optsApplied.disableRepartition {
		return k.newChildStream(node)
	}

	return k.newChildStream(node).maybeRepartitioned(
		rePartitionedWithSourceOpts(
			ConsumeWithAutoTopicCreateEnabled(
				PartitionAs(k.source()))))
}

func (k *kStream) Filter(filter processors.FilterFunc, opts ...StreamOption) Stream {
	node := &processors.Filter{
		FilterFunc: filter,
	}
	applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	return k.newChildStream(node)
}

func (k *kStream) Each(eachFunc processors.EachFunc, opts ...StreamOption) Stream {
	node := &processors.Each{
		EachFunc: eachFunc,
	}
	applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	return k.newChildStream(node)
}

// SelectKey select change the key of the stream by selecting a key form its value. Useful when joining streams with
// foreign keys
// Note: Applying Joins or other stateful operations after this will mark the stream for repartitioning
func (k *kStream) SelectKey(selectKeyFunc processors.SelectKeyFunc, opts ...StreamOption) Stream {
	node := &processors.KeySelector{
		SelectKeyFunc: selectKeyFunc,
	}

	optsApplied := applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	if optsApplied.disableRepartition {
		return k.newChildStream(node)
	}

	return k.newChildStream(node).maybeRepartitioned(
		rePartitionedWithSourceOpts(
			ConsumeWithAutoTopicCreateEnabled(
				PartitionAs(k.kSource))))
}

func (k *kStream) FlatMap(flatMapFunc processors.FlatMapFunc, opts ...StreamOption) Stream {
	node := &processors.FlatMap{
		FlatMapFunc: flatMapFunc,
	}

	applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	return k.newChildStream(node).maybeRepartitioned(
		rePartitionedDueTo(node, k.source()),
		rePartitionedWithSourceOpts(
			ConsumeWithAutoTopicCreateEnabled(
				PartitionAs(k.kSource))))
}

func (k *kStream) FlatMapValues(flatMapFunc processors.FlatMapValuesFunc, opts ...StreamOption) Stream {
	node := &processors.FlatMapValues{
		FlatMapValuesFunc: flatMapFunc,
	}

	applyNodeOptions(node, opts)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, node)

	return k.newChildStream(node)
}

func (k *kStream) Aggregate(store string, aggregatorFunc processors.AggregatorFunc, opts ...AggregateOpt) Table {
	optApplyier := new(AggregateOpts)
	optApplyier.encoders.key = k.keyEncoder()
	optApplyier.encoders.val = k.valEncoder()
	optApplyier.storeBuilderOpts = append(optApplyier.storeBuilderOpts, state_stores.WithChangelogOptions(
		state_stores.ChangelogWithTopicReplicaCount(k.builder.config.Store.Changelog.ReplicaCount),
	))
	optApplyier.apply(opts...)

	if !k.rePartitioned && k.repartitionOpts != nil {
		return k.Repartition(``, append(k.repartitionOpts.Apply(), rePartitionedDueTo(k.node(), k.source()))...).aggregate(store, aggregatorFunc, optApplyier)
	}

	return k.aggregate(store, aggregatorFunc, optApplyier)
}

type AggregateOpts struct {
	streamOpts       []StreamOption
	storeBuilderOpts []state_stores.StoreBuilderOption
	encoders         struct {
		key, val encoding.Encoder
	}
}

func (aggOpts *AggregateOpts) apply(opts ...AggregateOpt) {
	// apply default
	for _, opt := range opts {
		opt(aggOpts)
	}
}

type AggregateOpt func(opts *AggregateOpts)

func AggregateWithKeyEncoder(encoder encoding.Encoder) AggregateOpt {
	return func(opts *AggregateOpts) {
		opts.encoders.key = encoder
	}
}

func AggregateWithValEncoder(encoder encoding.Encoder) AggregateOpt {
	return func(opts *AggregateOpts) {
		opts.encoders.val = encoder
	}
}

func AggregateWithStoreOptions(options ...state_stores.StoreBuilderOption) AggregateOpt {
	return func(opts *AggregateOpts) {
		opts.storeBuilderOpts = options
	}
}

func AggregateWithStreamOptions(streamOptions ...StreamOption) AggregateOpt {
	return func(opts *AggregateOpts) {
		opts.streamOpts = streamOptions
	}
}

func (k *kStream) aggregate(store string, aggregatorFunc processors.AggregatorFunc, optApplyier *AggregateOpts) Table {
	node := &processors.Aggregator{
		AggregatorFunc: aggregatorFunc,
		Store:          store,
	}

	applyNodeOptions(node, optApplyier.streamOpts)

	storeBuilder := state_stores.NewStoreBuilder(
		node.Store,
		optApplyier.encoders.key,
		optApplyier.encoders.val,
		append([]state_stores.StoreBuilderOption{
			state_stores.StoreBuilderWithStoreOption(
				stores.WithBackendBuilder(k.builder.builders.backend)),
			state_stores.WithChangelogOptions(
				state_stores.ChangelogWithTopicTopicNameFormatter(k.builder.config.ChangelogTopicNameFormatter),
			),
		}, optApplyier.storeBuilderOpts...)...,
	)

	k.topology().AddStore(storeBuilder)

	k.addNode(node)

	return &kTableStream{
		kStream: k.newChildStream(node,
			childStreamWithKeyEncoder(optApplyier.encoders.key),
			childStreamWithValEncoder(optApplyier.encoders.val),
		),
		store: storeBuilder,
	}
}

func (k *kStream) JoinGlobalTable(table GlobalTable, keyMapper processors.KeyMapper, valMapper processors.JoinValueMapper, opts ...JoinOption) Stream {
	joinOpts := new(JoinOptions)
	joinOpts.apply(opts...)

	joiner := &processors.GlobalTableJoiner{
		JoinType:           processors.InnerJoin,
		Store:              table.Store().Name(),
		KeyMapper:          keyMapper,
		ValueMapper:        valMapper,
		RightKeyLookupFunc: joinOpts.rightLookupFunc,
	}

	applyNodeOptions(joiner, joinOpts.streamOptions)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, joiner)

	return k.newChildStream(joiner)
}

func (k *kStream) LeftJoinGlobalTable(table GlobalTable, keyMapper processors.KeyMapper, valMapper processors.JoinValueMapper, opts ...JoinOption) Stream {
	joinOpts := new(JoinOptions)
	joinOpts.apply(opts...)

	joiner := &processors.GlobalTableJoiner{
		JoinType:    processors.LeftJoin,
		Store:       table.Store().Name(),
		KeyMapper:   keyMapper,
		ValueMapper: valMapper,
	}

	applyNodeOptions(joiner, joinOpts.streamOptions)
	k.stpBuilder.AddNodeWithEdge(k.rootNode, joiner)

	return k.newChildStream(joiner)
}

func (k *kStream) JoinTable(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Stream {
	typ := processors.InnerJoin
	if k.repartitionOpts != nil {
		return k.Repartition(``, k.repartitionOpts.Apply()...).joinTable(table.(Table), valMapper, typ, opts...)
	}

	return k.joinTable(table, valMapper, typ, opts...)
}

func (k *kStream) LeftJoinTable(table Table, valMapper processors.JoinValueMapper, opts ...JoinOption) Stream {
	typ := processors.LeftJoin
	if k.repartitionOpts != nil {
		return k.Repartition(``, k.repartitionOpts.Apply()...).joinTable(table.(Table), valMapper, typ, opts...)
	}

	return k.joinTable(table, valMapper, typ, opts...)
}

func (k *kStream) joinTable(table Table, valMapper processors.JoinValueMapper, typ processors.JoinerType, opts ...JoinOption) Stream {
	// Mark source for co partitioning
	table.source().ShouldCoPartitionedWith(k.source())

	joinOpts := new(JoinOptions)
	joinOpts.apply(opts...)

	k.stpBuilder.AddStore(table.stateStore())

	leftJoiner := &processors.StreamJoiner{
		CurrentSide:         processors.LeftSide,
		OtherSideRequired:   typ == processors.InnerJoin,
		OtherStoreName:      table.stateStore().Name(),
		ValueMapper:         valMapper,
		OtherSideLookupFunc: joinOpts.rightLookupFunc,
	}

	applyNodeOptions(leftJoiner, joinOpts.streamOptions)

	// If sub topologies are different merge the other stream to current
	if k.topology().Id() != table.topology().Id() {
		k.merge(table)
	}

	k.topology().AddNodeWithEdge(k.node(), leftJoiner)

	return k.newChildStream(leftJoiner)
}

func (k *kStream) ToTable(store string, options ...TableOpt) Table {
	tblOpts := newTableOptsApplier(k.builder.config)
	tblOpts.encoders.key = k.keyEncoder()
	tblOpts.encoders.val = k.valEncoder()
	tblOpts.apply(options...)

	if tblOpts.useSourceAsChangelog {
		tblOpts.storeBuilderOpts = append(
			tblOpts.storeBuilderOpts,
			state_stores.WithChangelogOptions(
				state_stores.ChangelogWithSourceTopic(k.kSource.Topic()),
			),
			// If current stream source topic is used as source, disable logging to prevent circular message loop
			state_stores.LoggingDisabled(),
		)
	}

	stor := state_stores.NewStoreBuilder(
		store,
		tblOpts.encoders.key,
		tblOpts.encoders.val,
		append([]state_stores.StoreBuilderOption{
			state_stores.StoreBuilderWithStoreOption(stores.WithBackendBuilder(k.builder.builders.backend)),
			state_stores.WithChangelogOptions(
				state_stores.ChangelogWithTopicTopicNameFormatter(k.builder.config.ChangelogTopicNameFormatter),
			),
		}, tblOpts.storeBuilderOpts...)...,
	)
	k.stpBuilder.AddStore(stor)
	nd := &processors.TableConverter{
		Store: stor.Name(),
	}
	applyNodeOptions(nd, tblOpts.streamOpts)

	k.stpBuilder.AddNodeWithEdge(k.rootNode, nd)

	stream := k.newChildStream(nd)
	table := &kTableStream{
		kStream: stream,
		store:   stor,
	}

	return table
}

func (k *kStream) Through(topic string, options ...DslOption) Stream {
	dslOpts := new(DslOptions)
	dslOpts.kSinkOptions = []KSinkOption{
		ProduceWithLogger(k.builder.config.Logger),
	}
	dslOpts.apply(options...)

	k.To(topic, dslOpts.kSinkOptions...)

	// create a new sub stpBuilder
	sbTp := k.tpBuilder.NewKSubTopologyBuilder(topology.KindStream)
	source := NewKSource(topic, dslOpts.kSourceOptions...)

	sbTp.AddSource(source)

	return &kStream{
		tpBuilder:  k.tpBuilder,
		stpBuilder: sbTp,
		kSource:    source,
		rootNode:   source,
		builder:    k.builder,
		encoders:   struct{ key, val encoding.Encoder }{key: source.Encoder().Key, val: source.Encoder().Value},
	}
}

func (k *kStream) Merge(stream Stream) Stream {
	stream.source().ShouldCoPartitionedWith(k.source())

	// if merging streams topology is different to current one extract and import that first
	if k.stpBuilder.Id() != stream.topology().Id() {
		k.stpBuilder.MergeSubTopology(stream.topology())
		k.tpBuilder.RemoveSubTopology(stream.topology())
		// replace the other streams sub topology with merged sub topology
		stream.setSubTopology(k.stpBuilder)
	}

	merger := new(processors.Merger)
	k.stpBuilder.AddNodeWithEdge(k.node(), merger)
	k.stpBuilder.AddNodeWithEdge(stream.node(), merger)

	return k.newChildStream(merger)
}

func (k *kStream) To(topic string, options ...KSinkOption) {
	opts := []KSinkOption{
		ProduceWithLogger(k.builder.config.Logger),
		ProduceWithKeyEncoder(k.keyEncoder()),
		ProduceWithValEncoder(k.valEncoder()),
	}

	if k.builder.config.DefaultPartitioner != nil {
		opts = append(opts, ProduceWithPartitioner(k.builder.config.DefaultPartitioner))
	}

	sink := NewKSinkBuilder(topic, append(opts, options...)...)

	k.stpBuilder.AddNodeWithEdge(k.rootNode, sink)
}

func (k *kStream) Repartition(topic string, opts ...RepartitionOpt) Stream {
	rpOpts := new(RepartitionOpts)
	rpOpts.topicNameFormatter = k.builder.config.RepartitionTopicNameFormatter
	// Use current streams encoders as default encoders if not provided
	rpOpts.sourceOpts = append(rpOpts.sourceOpts,
		ConsumeWithKeyEncoder(k.keyEncoder()),
		ConsumeWithValEncoder(k.valEncoder()),
		ConsumeWithAutoTopicCreateEnabled(
			WithReplicaCount(k.builder.config.InternalTopicsDefaultReplicaCount),
			PartitionAs(k.source()),
		),
	)

	rpOpts.sinkOpts = append(rpOpts.sinkOpts,
		ProduceWithKeyEncoder(k.keyEncoder()),
		ProduceWithValEncoder(k.valEncoder()),
	)

	// If marked for repartition apply those options as well
	if k.markedForRepartition() {
		opts = append(k.repartitionOpts.Apply(), opts...)
	}

	if topic != `` {
		opts = append(opts, RePartitionAs(topic))
	}

	rpOpts.apply(opts...)

	if rpOpts.topicName == `` {
		panic(fmt.Sprintf(`Stream (%s) should repartition due to %s(%s)`,
			k.node(),
			rpOpts.dueTo.node.Type().Name,
			rpOpts.dueTo.node,
		))
	}

	// Source options overrides
	srcOpts := append([]KSourceOption{
		ConsumeWithTopicNameFormatterFunc(rpOpts.topicNameFormatter),
		markAsInternal(),
	}, rpOpts.sourceOpts...)

	// Sink options overrides
	sinkOpts := append([]KSinkOption{
		ProduceWithTopicNameFormatter(rpOpts.topicNameFormatter),
	}, rpOpts.sinkOpts...)

	through := k.Through(rpOpts.topicName,
		DslOptsSourceOptions(srcOpts...),
		DslOptsSinkOptions(sinkOpts...),
	).(*kStream)

	through.rePartitioned = true

	return through
}

func (k *kStream) AddStateStore(name string, keyEnc, valEnc encoding.Encoder, options ...state_stores.StoreBuilderOption) {
	opts := append([]state_stores.StoreBuilderOption{
		state_stores.StoreBuilderWithStoreOption(stores.WithBackendBuilder(k.builder.builders.backend)),
		state_stores.WithChangelogOptions(
			state_stores.ChangelogWithTopicTopicNameFormatter(k.builder.config.ChangelogTopicNameFormatter),
			state_stores.ChangelogWithTopicReplicaCount(k.builder.config.InternalTopicsDefaultReplicaCount),
		),
	}, options...)

	k.stpBuilder.AddStore(state_stores.NewStoreBuilder(
		name,
		keyEnc,
		valEnc,
		opts...,
	))
}

func (k *kStream) topology() topology.SubTopologyBuilder {
	return k.stpBuilder
}

func (k *kStream) setSubTopology(topology topology.SubTopologyBuilder) {
	k.stpBuilder = topology
}

func (k *kStream) node() topology.NodeBuilder {
	return k.rootNode
}

func (k *kStream) source() topology.Source {
	return k.kSource
}

func (k *kStream) addNode(node topology.NodeBuilder) {
	k.stpBuilder.AddNodeWithEdge(k.node(), node)
}

func (k *kStream) merge(stream StreamTopology) {
	k.stpBuilder.MergeSubTopology(stream.topology())
	k.tpBuilder.RemoveSubTopology(stream.topology())
	// replace the other streams sub topology with merged sub topology
	stream.setSubTopology(k.topology())
}

func (k *kStream) markedForRepartition() bool {
	return k.repartitionOpts != nil && (k.repartitionOpts.must != nil || k.repartitionOpts.mayBe != nil)
}

func (k *kStream) keyEncoder() encoding.Encoder {
	if k.encoders.key != nil {
		return k.encoders.key
	}

	return k.source().Encoder().Key
}

func (k *kStream) valEncoder() encoding.Encoder {
	if k.encoders.val != nil {
		return k.encoders.val
	}

	return k.source().Encoder().Value
}
