package processors

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/topology"
	"reflect"
	"testing"
)

func TestFilter_Run(t *testing.T) {
	tests := []struct {
		name        string
		kIn, vIn    interface{}
		kIOut, vOut interface{}
		funct       FilterFunc
		cont        bool
		error       error
		wantErr     bool
	}{
		{
			name: "filter-key-filtered",
			kIn:  100, vIn: 100,
			kIOut: nil, vOut: nil,
			funct: func(ctx context.Context, key, value interface{}) (bool, error) {
				return key.(int) == 99, nil
			},
			cont:    false,
			error:   nil,
			wantErr: false,
		},
		{
			name: "filter-value-filtered",
			kIn:  100, vIn: 100,
			kIOut: nil, vOut: nil,
			funct: func(ctx context.Context, key, value interface{}) (bool, error) {
				return value.(int) == 99, nil
			},
			cont:    false,
			error:   nil,
			wantErr: false,
		},
		{
			name: "filter-nil-key-val-on-error",
			kIn:  100, vIn: 100,
			kIOut: nil, vOut: nil,
			funct: func(ctx context.Context, key, value interface{}) (bool, error) {
				return false, errors.New(`some error`)
			},
			cont:    false,
			error:   errors.New(`some error`),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Filter{
				id:         topology.NewNodeId(1, `test`),
				FilterFunc: tt.funct,
			}
			gotKOut, gotVOut, gotNext, err := f.Run(context.Background(), tt.kIn, tt.vIn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotKOut, tt.kIOut) {
				t.Errorf("Run() gotKOut = %v, want %v", gotKOut, tt.kIOut)
			}
			if !reflect.DeepEqual(gotVOut, tt.vOut) {
				t.Errorf("Run() gotVOut = %v, want %v", gotVOut, tt.vOut)
			}
			if gotNext != tt.cont {
				t.Errorf("Run() gotNext = %v, want %v", gotNext, tt.cont)
			}
		})
	}
}
