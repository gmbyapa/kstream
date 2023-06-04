package topology

type Topology interface {
	SubTopologies() []SubTopologyBuilder
	SubTopology(id SubTopologyId) SubTopologyBuilder
	SubTopologyByTopic(topic string) SubTopologyBuilder
	StreamTopologies() SubTopologyBuilders
	SourceByTopic(topic string) Source
	GlobalTableTopologies() SubTopologyBuilders
	Describe() string
}
