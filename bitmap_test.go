package bbolt

import (
	"reflect"
	"testing"
)

func Test_bitmap(t *testing.T) {
	bm := newBitmap()
	bm.Add(1)
	bm.Add(2)
	bm.Add(3)
	bm.Add(3)
	bm.Add(300)
	bm.Add(30)
	res := []int{1, 2, 3, 30, 300}

	if got := bm.ToIndices(); !reflect.DeepEqual(got, res) {
		t.Errorf("Test_bitmap1() = %v, want %v", got, res)
	}

	if got := bm.Get(); got != 1 {
		t.Errorf("Test_bitmap2() = %v, want %v", got, 1)
	}
	bm.Del(1)
	if got := bm.Get(); got != 2 {
		t.Errorf("Test_bitmap3() = %v, want %v", got, 2)
	}
	bm.Del(2)

	if got := bm.Get(); got != 3 {
		t.Errorf("Test_bitmap4() = %v, want %v", got, 3)
	}
	bm.Del(3)

	if got := bm.Get(); got != 30 {
		t.Errorf("Test_bitmap5() = %v, want %v", got, 30)
	}

	bm.Del(30)

	if got := bm.Get(); got != 300 {
		t.Errorf("Test_bitmap6() = %v, want %v", got, 300)
	}

	if got := bm.Has(4); got != false {
		t.Errorf("Test_bitmap7() = %v, want %v", got, false)
	}

	if got := bm.Has(300); got != true {
		t.Errorf("Test_bitmap8() = %v, want %v", got, true)
	}

	bm.Del(300)

	bm.Add(64)
	if got := bm.Get(); got != 64 {
		t.Errorf("Test_bitmap9() = %v, want %v", got, 64)
	}
	bm.Del(64)
	bm.Del(43)

	var empty []int
	if got := bm.ToIndices(); !reflect.DeepEqual(got, empty) {
		t.Errorf("Test_bitmap10() = %v, want %v", got, empty)
	}

}

func Test_getRightMostSetBit(t *testing.T) {
	type args struct {
		n uint64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test1",
			args: args{
				n: 1,
			},
			want: 0,
		},
		{
			name: "test2",
			args: args{
				n: 3,
			},
			want: 0,
		},
		{
			name: "test3",
			args: args{
				n: 4,
			},
			want: 2,
		},
		{
			name: "test3",
			args: args{
				n: 0,
			},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getRightMostSetBit(tt.args.n); got != tt.want {
				t.Errorf("getRightMostSetBit() = %v, want %v", got, tt.want)
			}
		})
	}
}
