package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/streams/topology"
)

type kTopology struct {
	topologyBuilder topology.Builder
}

func (k *kTopology) SubTopologies() []topology.SubTopologyBuilder {
	return k.topologyBuilder.SubTopologies()
}

func (k *kTopology) StreamTopologies() topology.SubTopologyBuilders {
	var tps []topology.SubTopologyBuilder
	for _, sTp := range k.SubTopologies() {
		if sTp.Kind() == topology.KindStream || sTp.Kind() == topology.KindTable {
			tps = append(tps, sTp)
		}
	}

	return tps
}

func (k *kTopology) SourceByTopic(topic string) topology.Source {
	for _, subTp := range k.StreamTopologies() {
		for _, source := range subTp.Sources() {
			if source.Topic() == topic {
				return source
			}
		}
	}

	return nil
}

func (k *kTopology) GlobalTableTopologies() topology.SubTopologyBuilders {
	var tps []topology.SubTopologyBuilder
	for _, sTp := range k.SubTopologies() {
		if sTp.Kind() == topology.KindGlobalTable {
			tps = append(tps, sTp)
		}
	}

	return tps
}

func (k *kTopology) SubTopology(id topology.SubTopologyId) topology.SubTopologyBuilder {
	for _, sTp := range k.SubTopologies() {
		if sTp.Id() == id {
			return sTp
		}
	}

	panic(fmt.Sprintf(`Subtopology [%s] does not exist`, id))
}

func (k *kTopology) SubTopologyByTopic(topic string) topology.SubTopologyBuilder {
	for _, sTp := range k.SubTopologies() {
		for _, src := range sTp.Sources() {
			if src.Topic() == topic {
				return sTp
			}
		}
	}

	panic(fmt.Sprintf(`Subtopology with topic [%s] does not exist`, topic))
}

func (k *kTopology) Describe() string {
	viz := topology.NewTopologyVisualizer()
	viz.AddTopology(k.topologyBuilder)

	st, err := viz.Visualize()
	if err != nil {
		panic(err)
	}

	return st
}
