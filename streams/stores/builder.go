package stores

import (
	"github.com/gmbyapa/kstream/streams/encoding"
)

type StoreBuilder interface {
	Name() string
	Build(name string, options ...Option) (Store, error)
}

type defaultStoreBuilder struct {
	name           string
	keyEnc, valEnc encoding.Encoder
	options        []Option
}

func NewDefaultStoreBuilder(name string, keyEncoder encoding.Encoder, valEncoder encoding.Encoder, options ...Option) StoreBuilder {
	return &defaultStoreBuilder{
		name:    name,
		keyEnc:  keyEncoder,
		valEnc:  valEncoder,
		options: options,
	}
}

func (d *defaultStoreBuilder) Name() string {
	return d.name
}

func (d *defaultStoreBuilder) Build(name string, options ...Option) (Store, error) {
	if name == `` {
		name = d.name
	}
	return NewStore(name, d.keyEnc, d.valEnc, append(d.options, options...)...)
}
