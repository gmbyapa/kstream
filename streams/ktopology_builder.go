package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
)

type kTopologyBuilder struct {
	subTopologies []topology.SubTopologyBuilder
	storeRegistry stores.Registry
	stores        map[string]topology.LoggableStateStore
	logger        log.Logger
	counter       int
}

func NewKTopologyBuilder(storeRegistry stores.Registry, logger log.Logger) topology.Builder {
	return &kTopologyBuilder{
		storeRegistry: storeRegistry,
		logger:        logger,
		counter:       1,
	}
}

func (k *kTopologyBuilder) NewKSubTopologyBuilder(kind topology.SubTopologyKind) topology.SubTopologyBuilder {
	tp := &kSubTopologyBuilder{
		kind:   kind,
		stores: map[string]topology.LoggableStoreBuilder{},
	}
	k.addSubTopology(tp)

	return tp
}

func (k *kTopologyBuilder) RemoveSubTopology(builder topology.SubTopologyBuilder) {
	var subTps []topology.SubTopologyBuilder
	for _, subTp := range k.subTopologies {
		if subTp.Id() == builder.Id() {
			continue
		}
		subTps = append(subTps, subTp)
	}

	k.subTopologies = subTps
}

func (k *kTopologyBuilder) AddStore(store topology.LoggableStateStore) {
	k.stores[store.Name()] = store
}

func (k *kTopologyBuilder) StateStores() map[string]topology.LoggableStoreBuilder {
	stors := map[string]topology.LoggableStoreBuilder{}
	for _, subTp := range k.SubTopologies() {
		for _, store := range subTp.StateStores() {
			stors[store.Name()] = store
		}
	}

	return stors
}

func (k *kTopologyBuilder) Build(ctx topology.BuilderContext) (topology.Topology, error) {
	// Setup SubTopologies
	if err := k.setup(ctx); err != nil {
		return nil, errors.Wrap(err, `TopologyBuilder setup failed`)
	}

	// Tell admin client to create all the topics marked by changeslogs and source nodes
	k.logger.Info(`Creating internal topics`)
	if err := ctx.Admin().ApplyConfigs(); err != nil {
		return nil, errors.Wrap(err, `config apply failed`)
	}

	return &kTopology{topologyBuilder: k}, nil
}

func (k *kTopologyBuilder) Reset(ctx topology.BuilderContext) error {
	k.logger.Warn(`KStream resetting...`)
	defer k.logger.Warn(`KStream resetted`)

	// Setup SubTopologies
	if err := k.setup(ctx); err != nil {
		return err
	}

	// Get all the internal topics(eg:repartition topics) and store changelog topics
	var topics []string
	for _, subTp := range k.subTopologies {
		for _, str := range subTp.StateStores() {
			if str.Changelog().Internal() {
				topics = append(topics, str.Changelog().Topic())
			}
		}

		for _, src := range subTp.Sources() {
			if src.AutoCreate() {
				topics = append(topics, src.Topic())
			}
		}
	}

	k.logger.Warn(fmt.Sprintf(`Deleting topics %v`, topics))

	return ctx.Admin().DeleteTopics(topics)
}

func (k *kTopologyBuilder) setup(ctx topology.BuilderContext) error {
	// fetch meta info for sink and source topics (this doesn't include changelog topics)
	topicMap := map[string]*kafka.TopicConfig{}
	for _, subTp := range k.SubTopologies() {
		// Global topologies does not have any auto create options
		// or co partitioning requirements
		if subTp.Kind() == topology.KindGlobalTable {
			continue
		}

		for _, nd := range subTp.Nodes() {
			switch sc := nd.(type) {
			case topology.Source:
				if _, ok := topicMap[sc.Topic()]; !ok {
					topicMap[sc.Topic()] = new(kafka.TopicConfig)
				}

				topicMap[sc.Topic()].AutoCreate = sc.AutoCreate()
				topicMap[sc.Topic()].Internal = sc.Internal()
			case topology.SinkBuilder:
				if _, ok := topicMap[sc.Topic()]; !ok {
					topicMap[sc.Topic()] = new(kafka.TopicConfig)
					topicMap[sc.Topic()].AutoCreate = sc.AutoCreate()
				}
			}
		}
	}

	var topics []string
	for topic, info := range topicMap {
		if !info.AutoCreate {
			topics = append(topics, topic)
		}
	}

	var tpInfo map[string]*kafka.Topic

	// If topology has only GlobalTables there's nothing to check
	if len(topics) > 0 {
		info, err := ctx.Admin().FetchInfo(topics)
		if err != nil {
			return errors.Wrapf(err, `Metadata fetch failed. Topics %+v`, topics)
		}

		tpInfo = info
	}

	for _, info := range tpInfo {
		if info.Error != nil {
			return errors.Wrap(info.Error, fmt.Sprintf(`Metadata fetch failed on topic %s`, info.Name))
		}
		topicMap[info.Name].NumPartitions = info.NumPartitions
	}

	// Go through each sub topology to find source and sink topics
	for _, subTopology := range k.subTopologies {
		var maxPartitions int32
		var autoMaxPartitions int32
		for _, nd := range subTopology.Nodes() {
			// Global topologies does not have any auto create options
			// or co partitioning requirements
			if subTopology.Kind() == topology.KindGlobalTable {
				break
			}

			switch s := nd.(type) {
			case topology.Source:
				// Marked as AutoCreate has to be excluded(yet to be created)
				if topicMap[s.Topic()].AutoCreate {
					// In each sub topology if the topology contains multiple topics they have to be co-partitioned
					// including auto generated(eg: changelogs, repartitioned) topics. In this case if
					// nominated-source-topic(RePartitionedAs) partition count is greater than current autoMaxPartitions
					// then the autoMaxPartitions has to be adjusted to match the RePartitionedAs topic
					if s.RePartitionedAs() != nil {
						if tpInfo[s.RePartitionedAs().Topic()].NumPartitions > autoMaxPartitions {
							autoMaxPartitions = tpInfo[s.RePartitionedAs().Topic()].NumPartitions
						}
						topicMap[s.Topic()].NumPartitions = autoMaxPartitions
					}

					continue
				}

				// Get the partition count
				if tpInfo[s.Topic()].NumPartitions > maxPartitions {
					maxPartitions = tpInfo[s.Topic()].NumPartitions
				}

				topicMap[s.Topic()].NumPartitions = tpInfo[s.Topic()].NumPartitions
				topicMap[s.Topic()].ReplicationFactor = tpInfo[s.Topic()].ReplicationFactor
				topicMap[s.Topic()].ConfigEntries = tpInfo[s.Topic()].ConfigEntries

			case topology.SinkBuilder:
				if topicMap[s.Topic()].AutoCreate {
					continue
				}

				topicMap[s.Topic()].NumPartitions = tpInfo[s.Topic()].NumPartitions
				topicMap[s.Topic()].ReplicationFactor = tpInfo[s.Topic()].ReplicationFactor
				topicMap[s.Topic()].ConfigEntries = tpInfo[s.Topic()].ConfigEntries
			}
		}

		// If maxPartitions is zero that means only the auto generated topics there in the sub-topology
		if maxPartitions < 1 {
			maxPartitions = autoMaxPartitions
		}

		subTopologyBuilderCtx := topology.NewSubTopologySetupContext(ctx, topicMap, maxPartitions)
		if err := subTopology.Setup(subTopologyBuilderCtx); err != nil {
			return errors.Wrap(err, fmt.Sprintf(`sub topology[%s] setup error`, subTopology.Id().Name()))
		}
	}

	return nil
}

func (k *kTopologyBuilder) SubTopologies() topology.SubTopologyBuilders {
	return k.subTopologies
}

func (k *kTopologyBuilder) Describe() string {
	viz := topology.NewTopologyVisualizer()
	viz.AddTopology(k)

	st, err := viz.Visualize()
	if err != nil {
		panic(err)
	}

	return st
}

func (k *kTopologyBuilder) addSubTopology(builder topology.SubTopologyBuilder) {
	k.subTopologies = append(k.subTopologies, builder)
	id := k.counter
	builder.SetId(topology.NewSubTopologyId(
		id, fmt.Sprintf(`SUB-TOPOLOGY-%d`, id),
	))
	k.counter++
}
