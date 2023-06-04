package streams

//import (
//	"context"
//	"fmt"
//	"github.com/tryfix/streams/admin"
//	"github.com/tryfix/streams/backend/memory"
//	"github.com/tryfix/streams/consumer/adaptors/sarama/offsets"
//	"github.com/tryfix/streams/consumer/sarama_dept"
//	"github.com/tryfix/streams/data"
//	"github.com/tryfix/streams/streams/encoding"
//	"github.com/tryfix/streams/streams/store"
//	"github.com/tryfix/streams/streams/streams"
//	"github.com/tryfix/streams/producer/srm"
//	"github.com/tryfix/log"
//	"github.com/tryfix/metrics"
//	"strconv"
//	"sync"
//	"testing"
//	"time"
//)
//
//func TestGlobalTableStream_StartStreams(t *testing.T) {
//	initStream := func(startOffset GlobalTableOffset) (*globalTableStream, func(expectedCount int), func(start int, end int)) {
//		mocksTopics := admin.NewMockTopics()
//		kafkaAdmin := &admin.MockKafkaAdmin{
//			Topics: mocksTopics,
//		}
//		offsetManager := &offsets.MockManager{Topics: mocksTopics}
//
//		topics := make(map[string]*admin.Topic)
//		Stores := make(map[string]store.Store)
//		Tables := make(map[string]*streams.GlobalKTable)
//		opts := new(streams.globalTableOptions)
//		opts.backendWriter = streams.globalTableStoreWriter
//		opts.initialOffset = startOffset
//
//		for i := 0; i < 1; i++ {
//			name := fmt.Sprintf(`topic%d`, i)
//			topics[name] = &admin.Topic{
//				Name:              name,
//				NumPartitions:     100,
//				ReplicationFactor: 1,
//			}
//
//			conf := memory.NewConfig()
//			conf.logger = log.NewNoopLogger()
//			conf.MetricsReporter = metrics.NoopReporter()
//			stor, _ := store.NewStore(name, encoding.StringEncoder{}, encoding.StringEncoder{}, store.WithBackend(memory.NewMemoryBackend(conf)))
//			Stores[name] = stor
//			Tables[name] = &streams.GlobalKTable{store: stor, storeName: stor.Name(), Options: opts}
//		}
//
//		if err := kafkaAdmin.CreateTopics(topics); err != nil {
//			t.Error(err)
//		}
//
//		gTableStream, err := newGlobalTableStream(Tables, &GlobalTableStreamConfig{
//			ConsumerBuilder: sarama_dept.NewMockPartitionConsumerBuilder(mocksTopics, offsetManager),
//			BackendBuilder:  memory.Builder(memory.NewConfig()),
//			OffsetManager:   offsetManager,
//			KafkaAdmin:      kafkaAdmin,
//			Metrics:         metrics.NoopReporter(),
//			logger:          log.NewNoopLogger(),
//		})
//		if err != nil {
//			t.Error(err)
//		}
//
//		assertFunc := func(expectedCount int) {
//			count := 0
//			for _, str := range Stores {
//				i, _ := str.GetAll(nil)
//				for i.Valid() {
//					count++
//					i.Next()
//				}
//			}
//
//			if count != expectedCount*len(topics) {
//				t.Error(fmt.Sprintf(`invalid count have [%d]`, count))
//				t.Fail()
//			}
//		}
//
//		p := srm.NewMockProducer(mocksTopics)
//
//		producerFunc := func(start int, count int) {
//			for i := start; i <= count; i++ {
//				for topic := range topics {
//					_, _, _ = p.ProduceSync(nil, &data.Record{
//						Key:   []byte(fmt.Sprint(i)),
//						Value: []byte(`v`),
//						Topic: topic,
//					})
//				}
//			}
//		}
//
//		return gTableStream, assertFunc, producerFunc
//	}
//
//	t.Process(`NoMessage`, func(t *testing.T) {
//		gTableStream, assertFunc, producerFunc := initStream(streams.GlobalTableOffsetLatest)
//
//		producerFunc(0, 0)
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		assertFunc(0)
//	})
//
//	t.Process(`Latest`, func(t *testing.T) {
//		gTableStream, assertFunc, producerFunc := initStream(streams.GlobalTableOffsetLatest)
//
//		producerFunc(0, 3333)
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		assertFunc(0)
//	})
//
//	t.Process(`Oldest`, func(t *testing.T) {
//		gTableStream, assertFunc, producerFunc := initStream(streams.GlobalTableOffsetDefault)
//
//		producerFunc(0, 3332)
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		assertFunc(3333)
//	})
//
//	t.Process(`OldestAfterStarted`, func(t *testing.T) {
//		gTableStream, assertFunc, producerFunc := initStream(streams.GlobalTableOffsetDefault)
//
//		producerFunc(0, 3332)
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		time.Sleep(1 * time.Second)
//
//		producerFunc(3333, 6665)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		assertFunc(6666)
//	})
//
//	t.Process(`LatestAfterStarted`, func(t *testing.T) {
//		gTableStream, assertFunc, producerFunc := initStream(streams.GlobalTableOffsetLatest)
//
//		producerFunc(0, 3332)
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		time.Sleep(1 * time.Second)
//
//		producerFunc(3334, 6666)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		assertFunc(3333)
//	})
//}
//
//func TestGlobalKTable_Process(t *testing.T) {
//	initStream := func(opts *streams.globalTableOptions) (*globalTableStream, store.Store, func(key, value []byte)) {
//		mocksTopics := admin.NewMockTopics()
//		kafkaAdmin := &admin.MockKafkaAdmin{
//			Topics: mocksTopics,
//		}
//		offsetManager := &offsets.MockManager{Topics: mocksTopics}
//
//		topics := make(map[string]*admin.Topic)
//		Stores := make(map[string]store.Store)
//		Tables := make(map[string]*streams.GlobalKTable)
//
//		name := fmt.Sprintf(`topic_test_gt_process`)
//		topics[name] = &admin.Topic{
//			Name:              name,
//			NumPartitions:     100,
//			ReplicationFactor: 1,
//		}
//
//		conf := memory.NewConfig()
//		conf.logger = log.NewNoopLogger()
//		conf.MetricsReporter = metrics.NoopReporter()
//		stor, _ := store.NewStore(name, encoding.StringEncoder{}, encoding.StringEncoder{}, store.WithBackend(memory.NewMemoryBackend(conf)))
//		Stores[name] = stor
//		Tables[name] = &streams.GlobalKTable{store: stor, storeName: stor.Name(), Options: opts}
//
//		if err := kafkaAdmin.CreateTopics(topics); err != nil {
//			t.Error(err)
//		}
//
//		gTableStream, err := newGlobalTableStream(Tables, &GlobalTableStreamConfig{
//			ConsumerBuilder: sarama_dept.NewMockPartitionConsumerBuilder(mocksTopics, offsetManager),
//			BackendBuilder:  memory.Builder(memory.NewConfig()),
//			OffsetManager:   offsetManager,
//			KafkaAdmin:      kafkaAdmin,
//			Metrics:         metrics.NoopReporter(),
//			logger:          log.NewNoopLogger(),
//		})
//		if err != nil {
//			t.Error(err)
//		}
//
//		p := srm.NewMockProducer(mocksTopics)
//
//		producerFunc := func(key, value []byte) {
//			for topic := range topics {
//				_, _, _ = p.ProduceSync(nil, &data.Record{
//					Key:   key,
//					Value: value,
//					Topic: topic,
//				})
//			}
//		}
//
//		return gTableStream, stor, producerFunc
//	}
//
//	t.Process(`TestVersioningNoPreviousRecords`, func(t *testing.T) {
//		opts := new(streams.globalTableOptions)
//		opts.backendWriter = streams.globalTableStoreWriter
//		opts.recordVersionExtractor = func(ctx context.Context, key, value interface{}) (int64, error) {
//			val := value.(string)
//			return strconv.ParseInt(val, 10, 64)
//		}
//		opts.recordVersionComparator = func(newVersion, currentVersion int64) bool {
//			return newVersion > currentVersion
//		}
//		opts.initialOffset = streams.GlobalTableOffsetDefault
//
//		gTableStream, stor, producerFunc := initStream(opts)
//
//		//all records should sink because there is no previous records to be compared. Record count should be 2
//		producerFunc([]byte(`1`), []byte(`1`))
//		producerFunc([]byte(`2`), []byte(`2`))
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		time.Sleep(1 * time.Second)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		count := 0
//		i, _ := stor.GetAll(context.Background())
//		for i.Valid() {
//			count++
//			i.Next()
//		}
//
//		if count != 2 {
//			t.Error(fmt.Sprintf(`invalid count have [%d]`, count))
//			t.Fail()
//		}
//	})
//
//	t.Process(`TestVersioningSinkingCorrectOrder`, func(t *testing.T) {
//		opts := new(streams.globalTableOptions)
//		opts.backendWriter = streams.globalTableStoreWriter
//		opts.recordVersionExtractor = func(ctx context.Context, key, value interface{}) (int64, error) {
//			val := value.(string)
//			return strconv.ParseInt(val, 10, 64)
//		}
//		opts.recordVersionComparator = func(newVersion, currentVersion int64) bool {
//			return newVersion > currentVersion
//		}
//		opts.initialOffset = streams.GlobalTableOffsetDefault
//
//		gTableStream, stor, producerFunc := initStream(opts)
//
//		//all records should sink and last stored value for the key =1 should be 7 because all records versions are in order
//		producerFunc([]byte(`1`), []byte(`1`))
//		producerFunc([]byte(`1`), []byte(`5`))
//		producerFunc([]byte(`1`), []byte(`7`))
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		time.Sleep(1 * time.Second)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		value, _ := stor.Get(context.Background(), `1`)
//
//		if value != `7` {
//			t.Error(fmt.Sprintf(`invalid value have [%s] expected [7]`, value))
//			t.Fail()
//		}
//	})
//
//	t.Process(`TestVersioningSinkingIncorrectOrder`, func(t *testing.T) {
//		opts := new(streams.globalTableOptions)
//		opts.backendWriter = streams.globalTableStoreWriter
//		opts.recordVersionExtractor = func(ctx context.Context, key, value interface{}) (int64, error) {
//			val := value.(string)
//			return strconv.ParseInt(val, 10, 64)
//		}
//		opts.recordVersionComparator = func(newVersion, currentVersion int64) bool {
//			return newVersion > currentVersion
//		}
//		opts.initialOffset = streams.GlobalTableOffsetDefault
//
//		gTableStream, stor, producerFunc := initStream(opts)
//
//		//last record should not sink and last stored value should be 5 because last record's version is not greater than 5
//		producerFunc([]byte(`1`), []byte(`1`))
//		producerFunc([]byte(`1`), []byte(`5`))
//		producerFunc([]byte(`1`), []byte(`2`))
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		time.Sleep(1 * time.Second)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		value, _ := stor.Get(context.Background(), `1`)
//
//		if value != `5` {
//			t.Error(fmt.Sprintf(`invalid value have [%s] expected [5]`, value))
//			t.Fail()
//		}
//	})
//
//	t.Process(`TestVersioningDeleteAndReSink`, func(t *testing.T) {
//		opts := new(streams.globalTableOptions)
//		opts.backendWriter = streams.globalTableStoreWriter
//		opts.recordVersionExtractor = func(ctx context.Context, key, value interface{}) (int64, error) {
//			val := value.(string)
//			return strconv.ParseInt(val, 10, 64)
//		}
//		opts.recordVersionComparator = func(newVersion, currentVersion int64) bool {
//			return newVersion > currentVersion
//		}
//		opts.initialOffset = streams.GlobalTableOffsetDefault
//
//		gTableStream, stor, producerFunc := initStream(opts)
//
//		/*after delete previous records, new value's version no need to compare with previous versions because
//		no previous records in Store for that key
//		*/
//		producerFunc([]byte(`1`), []byte(`1`))
//		producerFunc([]byte(`1`), []byte(`5`))
//		producerFunc([]byte(`1`), nil) // to delete Store value for key = `1`
//		producerFunc([]byte(`1`), []byte(`2`))
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		time.Sleep(1 * time.Second)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		value, _ := stor.Get(context.Background(), `1`)
//
//		if value != `2` {
//			t.Error(fmt.Sprintf(`invalid value have [%s] expected [2]`, value))
//			t.Fail()
//		}
//	})
//
//	t.Process(`TestVersioningNoVersionExtractor`, func(t *testing.T) {
//		opts := new(streams.globalTableOptions)
//		opts.backendWriter = streams.globalTableStoreWriter
//		opts.initialOffset = streams.GlobalTableOffsetDefault
//
//		gTableStream, stor, producerFunc := initStream(opts)
//
//		//record should be sync in incorrect order if there is no version extractor
//		producerFunc([]byte(`1`), []byte(`1`))
//		producerFunc([]byte(`1`), []byte(`5`))
//		producerFunc([]byte(`1`), []byte(`2`))
//
//		wg := &sync.WaitGroup{}
//		gTableStream.StartStreams(wg)
//
//		time.Sleep(1 * time.Second)
//
//		go func() {
//			time.Sleep(1 * time.Second)
//			gTableStream.stop()
//		}()
//		wg.Wait()
//
//		value, _ := stor.Get(context.Background(), `1`)
//
//		if value != `2` {
//			t.Error(fmt.Sprintf(`invalid value have [%s] expected [2]`, value))
//			t.Fail()
//		}
//	})
//}
