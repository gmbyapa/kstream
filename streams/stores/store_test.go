package stores

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/streams/encoding"
	"reflect"
	"sort"
	"testing"
	"time"
)

func makeTestStore(t *testing.T, expiry time.Duration) Store {
	stor, err := NewStore(
		`test_store`,
		encoding.StringEncoder{},
		encoding.StringEncoder{},
		WithBackend(makeTestBackend(`test_backend`)))
	if err != nil {
		t.Error(err)
	}

	return stor
}

func TestDefaultStore_Get(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	err := st.Set(ctx, `100`, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, `100`)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_Get_Should_return_Nul_For_Invalid_Key(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	testKey := `100`
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, `200`)
	if err != nil {
		t.Error(err)
	}

	if v != nil {
		t.Fail()
	}
}

func TestDefaultStore_Set(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	testKey := `100`
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_Delete(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	testKey := `100`
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_GetAll_FirstToLast(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)

	var keyVals []string
	for i := 1; i <= 1000; i++ {
		key := fmt.Sprint(i)
		val := fmt.Sprintf(`%d`, i*10)
		keyVals = append(keyVals, fmt.Sprintf(`%s-%s`, key, val))
		if err := st.Set(ctx, key, val, 0); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(keyVals)

	i, err := st.Iterator(ctx)
	if err != nil {
		t.Error(err)
	}
	defer i.Close()

	var keyValsHave []string
	for i.SeekToFirst(); i.Valid(); i.Next() {
		key, err := i.Key()
		if err != nil {
			t.Error(err)
		}

		val, err := i.Value()
		if err != nil {
			t.Error(err)
		}
		keyValsHave = append(keyValsHave, fmt.Sprintf(`%s-%s`, key.(string), val.(string)))
	}

	sort.Strings(keyValsHave)
	if !reflect.DeepEqual(keyVals, keyValsHave) {
		t.Fail()
	}
}

func TestDefaultStore_Iterator(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)

	for i := 1; i <= 10; i++ {
		key := fmt.Sprint(i)
		val := fmt.Sprintf(`%d`, i*10)
		if err := st.Set(ctx, key, val, 0); err != nil {
			t.Fatal(err)
		}
	}

	i, err := st.Iterator(ctx)
	if err != nil {
		t.Error(err)
	}
	defer i.Close()

	var keysHave []string
	for i.SeekToFirst(); i.Valid(); i.Next() {
		key, err := i.Key()
		if err != nil {
			t.Error(err)
		}

		keysHave = append(keysHave, key.(string))
	}

	sort.Strings(keysHave)
	expected := []string{`1`, `10`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`}
	if !reflect.DeepEqual(
		expected, keysHave) {
		t.Logf("expected: \n%v\n have: \n%v", expected, keysHave)
		t.Fail()
	}
}

func TestDefaultStore_PrefixedIterator(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprint(i)
		val := fmt.Sprintf(`%d`, i*10)
		if err := st.Set(ctx, key, val, 0); err != nil {
			t.Fatal(err)
		}
	}

	i, err := st.PrefixedIterator(ctx, `10`, encoding.StringEncoder{})
	if err != nil {
		t.Error(err)
	}
	defer i.Close()

	var keysHave []string
	for i.SeekToFirst(); i.Valid(); i.Next() {
		key, err := i.Key()
		if err != nil {
			t.Error(err)
		}

		keysHave = append(keysHave, key.(string))
	}

	sort.Strings(keysHave)
	expected := []string{`10`, `100`, `1000`, `101`, `102`, `103`, `104`, `105`, `106`, `107`, `108`, `109`}
	if !reflect.DeepEqual(
		expected, keysHave) {
		t.Logf("expected: \n%v\n have: \n%v", expected, keysHave)
		t.Fail()
	}
}
