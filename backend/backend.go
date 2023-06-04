/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package backend

import (
	"time"
)

type Builder func(name string) (Backend, error)

type KeyVal struct {
	Key, Val []byte
}

type Backend interface {
	// Name returns the name of the store
	Name() string
	String() string
	Persistent() bool
	Close() error
	Reader
	Writer
	BatchWriter
}

type Reader interface {
	// Get looks for the value of a given key. Will return nil if the value does not exist
	Get(key []byte) ([]byte, error)
	PrefixedIterator(keyPrefix []byte) Iterator
	Iterator() Iterator
	Close() error
}

type Writer interface {
	// Set writes the given key:value pair with an optional expiry
	Set(key []byte, value []byte, expiry time.Duration) error
	Delete(key []byte) error
	Flush() error
	Close() error
}

type BatchWriter interface {
	SetAll(kayVals []KeyVal, expiry time.Duration) error
	Close() error
}

type CacheableBackend interface {
	Cache() Cache
}

type Cache interface {
	Reader
	Writer
	Reset()
}
