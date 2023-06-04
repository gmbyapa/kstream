package pebble

import "github.com/cockroachdb/pebble"

type Iterator struct {
	itr *pebble.Iterator
}

func (i *Iterator) SeekToFirst() {
	i.itr.First()
}

func (i *Iterator) SeekToLast() {
	i.itr.Last()
}

func (i *Iterator) Seek(key []byte) {
	i.itr.SeekGE(key)
}

func (i *Iterator) Next() {
	i.itr.Next()
}

func (i *Iterator) Prev() {
	i.itr.Prev()
}

func (i *Iterator) Close() {
	i.itr.Close()
}

func (i *Iterator) Key() []byte {
	return i.itr.Key()
}

func (i *Iterator) Value() []byte {
	return i.itr.Value()
}

func (i *Iterator) Valid() bool {
	return i.itr.Valid()
}

func (i *Iterator) Error() error {
	return i.itr.Error()
}
