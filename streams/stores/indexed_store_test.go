package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/pebble"
	"github.com/gmbyapa/kstream/streams/encoding"
	"os"
	"reflect"
	"testing"
)

func indexStoreTestStoreSetup(t *testing.T, idx IndexBuilder) IndexedStore {
	var bkBuilder backend.Builder = func(name string) (backend.Backend, error) {
		conf := pebble.NewConfig()
		dir, err := os.MkdirTemp(os.TempDir(), `*`)
		if err != nil {
			return nil, err
		}
		conf.Dir = dir

		return pebble.NewPebbleBackend(name, conf)

	}
	i, err := NewIndexedStore(
		`foo`,
		encoding.IntEncoder{},
		encoding.StringEncoder{},
		[]IndexBuilder{idx},
		WithBackendBuilder(bkBuilder),
	)
	if err != nil {
		t.Error(err)
	}

	return i
}

func indexStoreIndexSetup(t *testing.T) IndexBuilder {
	return buildIndexT(t)
}

func Test_indexedStore_Delete(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), 200, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Set(context.Background(), 300, `111,333`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Delete(context.Background(), 200); err != nil {
		t.Error(err)
	}

	itr, err := str.Indexes()[0].Read(`111`)
	if err != nil {
		t.Error(err)
	}

	var data []string
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		data = append(data, string(itr.Value()))
	}

	if !reflect.DeepEqual(data, []string{`300`}) {
		t.Errorf(`want []string{300}, have %#v`, data)
	}
}

func Test_indexedStore_Delete_Should_Remove_Indexed_Values(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), 200, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Set(context.Background(), 300, `111,333`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Delete(context.Background(), 200); err != nil {
		t.Error(err)
	}

	if err := str.Delete(context.Background(), 300); err != nil {
		t.Error(err)
	}

	itr, err := str.Indexes()[0].Read(`111`)
	if err != nil {
		t.Error(err)
	}

	var data [][]byte
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		data = append(data, itr.Value())
	}

	if len(data) > 0 {
		t.Errorf(`want []string{}, have %#v`, data)
	}
}

func Test_indexedStore_Set(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), 200, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Set(context.Background(), 300, `111,333`, 0); err != nil {
		t.Error(err)
	}

	itr, err := str.Indexes()[0].Read(`111`)
	if err != nil {
		t.Error(err)
	}

	var want []string
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		if string(itr.Value()) == `200` || string(itr.Value()) == `300` {
			want = append(want, string(itr.Value()))
		}
	}

	if len(want) < 2 {
		t.Fail()
	}
}

func Test_indexedStore_Set_OldIndexedValues_ShouldGetDeleted(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), 200, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Set(context.Background(), 200, `222,333`, 0); err != nil {
		t.Error(err)
	}

	itr, err := str.Indexes()[0].Read(`111`)
	if err != nil {
		t.Error(err)
	}

	var data [][]byte
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		data = append(data, itr.Value())
	}

	if len(data) > 0 {
		t.Fail()
	}
}

func TestIndexedStore_GetIndexedRecords(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), 200, `111,222`, 0); err != nil {
		t.Error(err)
	}
	if err := str.Set(context.Background(), 300, `111,333`, 0); err != nil {
		t.Error(err)
	}
	if err := str.Set(context.Background(), 400, `222,333`, 0); err != nil {
		t.Error(err)
	}

	itr, err := str.GetIndexedRecords(context.Background(), `foo`, `111`)
	if err != nil {
		t.Error(err)
	}

	var want []interface{}
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		val, err := itr.Value()
		if err != nil {
			t.Error(err)
		}

		if val.(string) == `111,222` || val.(string) == `111,333` {
			want = append(want, val)
		}
	}

	if len(want) < 2 {
		t.Fail()
	}
}
