package topology

type SubTopologyBuilders []SubTopologyBuilder

func (b SubTopologyBuilders) SourceTopics() []string {
	var tps []string
	for _, subTp := range b {
		for _, src := range subTp.Sources() {
			tps = append(tps, src.Topic())
		}
	}

	return tps
}

func (b SubTopologyBuilders) Topics() []string {
	var tps []string
	for _, subTp := range b {
		for _, nd := range subTp.Nodes() {
			switch srcSnk := nd.(type) {
			case Source:
				tps = append(tps, srcSnk.Topic())
			case SinkBuilder:
				tps = append(tps, srcSnk.Topic())
			}

		}
	}

	return tps
}

func (b SubTopologyBuilders) SourceTopicsFor(kind SubTopologyKind) []string {
	var tps []string
	for _, subTp := range b {
		if subTp.Kind() != kind {
			continue
		}
		for _, src := range subTp.Sources() {
			tps = append(tps, src.Topic())
		}
	}

	return tps
}

type Builder interface {
	NewKSubTopologyBuilder(kind SubTopologyKind) SubTopologyBuilder
	RemoveSubTopology(builder SubTopologyBuilder)
	SubTopologies() SubTopologyBuilders
	Build(ctx BuilderContext) (Topology, error)
	Describe() string
	Reset(ctx BuilderContext) error
}
