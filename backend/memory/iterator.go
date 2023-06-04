/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package memory

type Iterator struct {
	records    []ByteRecord
	currentKey int
	valid      bool
}

func NewMemoryIterator(records []ByteRecord) *Iterator {
	return &Iterator{
		records: records,
		valid:   len(records) > 0,
	}
}

func (i *Iterator) SeekToFirst() {
	i.currentKey = 0
}

func (i *Iterator) SeekToLast() {
	i.currentKey = len(i.records) - 1
}

func (i *Iterator) Seek(key []byte) {
	for idx, r := range i.records {
		if string(r.Key) == string(key) {
			i.currentKey = idx
		}
	}
}

func (i *Iterator) Next() {
	if i.currentKey == len(i.records)-1 {
		i.valid = false
		return
	}
	i.currentKey++
}

func (i *Iterator) Prev() {
	if i.currentKey <= 0 {
		i.valid = false
		return
	}
	i.currentKey--
}

func (i *Iterator) Close() {
	i.records = nil
}

func (i *Iterator) Key() []byte {
	return i.records[i.currentKey].Key
}

func (i *Iterator) Value() []byte {
	return i.records[i.currentKey].Value
}

func (i *Iterator) Valid() bool {
	return i.valid
}

func (i *Iterator) Error() error {
	return nil
}
