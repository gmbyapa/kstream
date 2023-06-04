package kafka

type PartitionConf struct {
	Id    int32
	Error error
}

type Topic struct {
	Name              string
	Partitions        []PartitionConf
	Error             error
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment map[int32][]int32
	ConfigEntries     map[string]string
}

type TopicConfig struct {
	NumPartitions     int32
	ReplicationFactor int16
	ConfigEntries     map[string]string
	Internal          bool
	AutoCreate        bool
}

type Admin interface {
	FetchInfo(topics []string) (map[string]*Topic, error)
	CreateTopics(topics []*Topic) error
	ListTopics() ([]string, error)
	ApplyConfigs() error
	StoreConfigs(topics []*Topic) error
	DeleteTopics(topics []string) error
	Close()
}
