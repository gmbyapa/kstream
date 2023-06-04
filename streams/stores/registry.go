package stores

import (
	"fmt"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
)

type StateStoresLister func() []ReadOnlyStore

type Registry interface {
	Register(store ReadOnlyStore) error
	RegisterDynamic(name string, lister StateStoresLister) error
	Create(name string, keyEncoder, valEncoder encoding.Encoder, options ...Option) (Store, error)
	CreateOrReturn(name string, keyEncoder encoding.Encoder, valEncoder encoding.Encoder, options ...Option) (Store, error)
	NewIndexedStore(name string, keyEncoder, valEncoder encoding.Encoder, indexes []IndexBuilder, options ...Option) IndexedStore
	NewBuilder(name string, keyEncoder, valEncoder encoding.Encoder, options ...Option) StoreBuilder
	Store(name string) (ReadOnlyStore, error)
	Builder(name string) (StoreBuilder, error)
	Index(name string) (Index, error)
	Stores() []ReadOnlyStore
	StartWebServer()
	Builders() []StoreBuilder
	Indexes() []Index
}

type registry struct {
	stores        map[string]ReadOnlyStore
	storeBuilders map[string]StoreBuilder
	dynamicStores map[string]StateStoresLister

	indexes             map[string]Index
	mu                  *sync.Mutex
	logger              log.Logger
	applicationId       string
	storeBuilder        Builder
	indexedStoreBuilder IndexedStoreBuilder
	http                struct {
		enable bool
		host   string
	}
}

type RegistryConfig struct {
	Host                string
	HttpEnabled         bool
	applicationId       string
	StoreBuilder        Builder
	IndexedStoreBuilder IndexedStoreBuilder
	Logger              log.Logger
	MetricsReporter     metrics.Reporter
}

func NewRegistry(config *RegistryConfig) Registry {
	reg := &registry{
		stores:              make(map[string]ReadOnlyStore),
		dynamicStores:       make(map[string]StateStoresLister),
		indexes:             make(map[string]Index),
		mu:                  &sync.Mutex{},
		logger:              config.Logger.NewLog(log.Prefixed(`store-registry`)),
		applicationId:       config.applicationId,
		indexedStoreBuilder: config.IndexedStoreBuilder,
		storeBuilder:        config.StoreBuilder,
	}

	reg.http.enable = config.HttpEnabled
	reg.http.host = config.Host

	return reg
}

func (r *registry) Register(store ReadOnlyStore) error {
	name := store.Name()
	// TODO check this later
	if _, ok := r.stores[name]; ok {
		return errors.Errorf(fmt.Sprintf(`store [%s] already exist`, name))
	}

	// if store is an IndexedStore register Indexes
	if stor, ok := store.(IndexedStore); ok {
		for _, idx := range stor.Indexes() {
			r.indexes[idx.String()] = idx
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stores[name] = store

	return nil
}

func (r *registry) RegisterDynamic(name string, lister StateStoresLister) error {
	if _, ok := r.dynamicStores[name]; ok {
		return errors.Errorf(fmt.Sprintf(`store [%s] already exist`, name))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.dynamicStores[name] = lister

	return nil
}

func (r *registry) Create(name string, keyEncoder encoding.Encoder, valEncoder encoding.Encoder, options ...Option) (Store, error) {
	if _, ok := r.stores[name]; ok {
		return nil, errors.Errorf(`store [%s] already exist`, name)
	}

	s, err := r.storeBuilder(name, keyEncoder, valEncoder, options...)
	if err != nil {
		r.logger.Fatal(err)
	}

	r.stores[name] = s

	return r.stores[name].(Store), nil
}

func (r *registry) CreateOrReturn(name string, keyEncoder encoding.Encoder, valEncoder encoding.Encoder, options ...Option) (Store, error) {
	if stor, ok := r.stores[name]; ok {
		return stor.(Store), nil
	}

	return r.Create(name, keyEncoder, valEncoder, options...)
}

func (r *registry) NewBuilder(name string, keyEncoder, valEncoder encoding.Encoder, options ...Option) StoreBuilder {
	if _, ok := r.storeBuilders[name]; ok {
		r.logger.Fatal(fmt.Sprintf(`storeBuilder [%s] already exist`, name))
	}

	r.storeBuilders[name] = NewDefaultStoreBuilder(name, keyEncoder, valEncoder, options...)

	return r.storeBuilders[name]
}

func (r *registry) NewIndexedStore(name string, keyEncoder, valEncoder encoding.Encoder, indexes []IndexBuilder, options ...Option) IndexedStore {
	if _, ok := r.stores[name]; ok {
		r.logger.Fatal(fmt.Sprintf(`Store [%s] already exist`, name))
	}

	s, err := r.indexedStoreBuilder(name, keyEncoder, valEncoder, indexes, options...)
	if err != nil {
		r.logger.Fatal(err)
	}

	r.stores[name] = s

	for _, idx := range s.Indexes() {
		r.indexes[idx.String()] = idx
	}

	return s
}

func (r *registry) Store(name string) (ReadOnlyStore, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	store, ok := r.stores[name]
	if !ok {
		// Check in dynamic stores
		for _, lister := range r.dynamicStores {
			for _, ins := range lister() {
				if ins.Name() == name {
					return ins, nil
				}
			}
		}

		return nil, errors.Errorf(`unknown store [%s]`, name)
	}

	return store, nil
}

func (r *registry) Builder(name string) (StoreBuilder, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	builder, ok := r.storeBuilders[name]
	if !ok {
		return nil, errors.Errorf(`unknown storeBuilder [%s]`, name)
	}

	return builder, nil
}

func (r *registry) Index(name string) (Index, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, ok := r.indexes[name]
	if !ok {
		return nil, errors.Errorf(`unknown index [%s]`, name)
	}

	return idx, nil
}

func (r *registry) Stores() []ReadOnlyStore {
	var list []ReadOnlyStore

	for _, stor := range r.stores {
		list = append(list, stor)
	}

	// Append dynamic stores
	for _, lister := range r.dynamicStores {
		list = append(list, lister()...)
	}

	return list
}

func (r *registry) Builders() []StoreBuilder {
	var list []StoreBuilder

	for _, stor := range r.storeBuilders {
		list = append(list, stor)
	}

	return list
}

func (r *registry) Indexes() []Index {
	var list []Index

	for _, idx := range r.indexes {
		list = append(list, idx)
	}

	return list
}

func (r *registry) StartWebServer() {
	if r.http.enable {
		MakeEndpoints(r.http.host, r, r.logger.NewLog(log.Prefixed(`http`)))
	}
}
