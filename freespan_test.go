package bbolt

import (
	"reflect"
	"testing"
)

func Test_mergeTwoSortedSpan(t *testing.T) {
	s1 := makeFreespan(pgid(3), uint64(5))
	s2 := makeFreespan(pgid(8), uint64(4))
	s3 := makeFreespan(pgid(34), uint64(4))
	s4 := makeFreespan(pgid(68), uint64(4))
	s5 := makeFreespan(pgid(400), uint64(40))
	s6 := makeFreespan(pgid(3000), uint64(4))
	s7 := makeFreespan(pgid(40000), uint64(40))
	s8 := makeFreespan(pgid(100000), uint64(4))
	s9 := makeFreespan(pgid(4), uint64(0))
	s10 := makeFreespan(pgid(5), uint64(4))
	s11 := makeFreespan(pgid(5), uint64(65534))
	s12 := makeFreespan(pgid(65539), uint64(4))

	type args struct {
		a []freespan
		b []freespan
	}
	tests := []struct {
		name    string
		args    args
		wantDst []freespan
	}{
		{
			name: "test1",
			args: args{
				a: []freespan{s1, s3},
				b: []freespan{s2, s4},
			},
			wantDst: []freespan{makeFreespan(pgid(3), 9), s3, s4},
		},
		{
			name: "test2",
			args: args{
				a: []freespan{s5},
				b: []freespan{s6, s7},
			},
			wantDst: []freespan{s5, s6, s7},
		},
		{
			name: "test3",
			args: args{
				a: []freespan{},
				b: []freespan{s6, s7},
			},
			wantDst: []freespan{s6, s7},
		},
		{
			name: "test4",
			args: args{
				a: []freespan{s6, s7},
				b: []freespan{},
			},
			wantDst: []freespan{s6, s7},
		},
		{
			name: "test5",
			args: args{
				a: []freespan{s6, s8},
				b: []freespan{s7},
			},
			wantDst: []freespan{s6, s7, s8},
		},
		{
			name: "test6",
			args: args{
				a: []freespan{s1},
				b: []freespan{s2},
			},
			wantDst: []freespan{makeFreespan(pgid(3), 9)},
		},
		{
			name: "test7",
			args: args{
				a: []freespan{s10},
				b: []freespan{s9},
			},
			wantDst: []freespan{s10},
		},
		{
			name: "test8",
			args: args{
				a: []freespan{s11},
				b: []freespan{s12},
			},
			wantDst: []freespan{s11, s12},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotDst := mergeTwoSortedSpan(tt.args.a, tt.args.b); !reflect.DeepEqual(gotDst, tt.wantDst) {
				t.Errorf("mergeTwoSortedSpan() = %v, want %v", gotDst, tt.wantDst)
			}
		})
	}
}

func Test_filterZeroFreespan(t *testing.T) {
	s1 := makeFreespan(pgid(100000), uint64(4))
	s2 := makeFreespan(pgid(4), uint64(0))
	type args struct {
		spans []freespan
	}
	tests := []struct {
		name    string
		args    args
		wantDst []freespan
	}{
		{
			name: "test1",
			args: args{
				[]freespan{s1, s2},
			},
			wantDst: []freespan{s1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotDst := filterZeroFreespan(tt.args.spans); !reflect.DeepEqual(gotDst, tt.wantDst) {
				t.Errorf("filterZeroFreespan() = %v, want %v", gotDst, tt.wantDst)
			}
		})
	}
}

func Test_mergenorm(t *testing.T) {
	type args struct {
		dst []freespan
		all [][]freespan
	}
	tests := []struct {
		name string
		args args
		want []freespan
	}{
		{
			name: "test1",
			args: args{
				dst: []freespan{},
				all: [][]freespan{
					{makeFreespan(12, 1), makeFreespan(39, 1)},
					{makeFreespan(11, 1), makeFreespan(28, 1)},
					{makeFreespan(3, 1)},
				},
			},
			want: []freespan{makeFreespan(3, 1), makeFreespan(11, 2), makeFreespan(28, 1), makeFreespan(39, 1)},
		},
		{
			name: "test2",
			args: args{
				dst: []freespan{makeFreespan(10, 1)},
				all: [][]freespan{
					{makeFreespan(12, 1), makeFreespan(39, 1)},
					{makeFreespan(11, 1), makeFreespan(28, 1)},
					{makeFreespan(3, 1)},
				},
			},
			want: []freespan{makeFreespan(3, 1), makeFreespan(10, 3), makeFreespan(28, 1), makeFreespan(39, 1)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergenorm(tt.args.dst, tt.args.all); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergenorm() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_filterZeroFreespanInPlace(t *testing.T) {
	type args struct {
		spans []freespan
	}
	tests := []struct {
		name string
		args args
		want []freespan
	}{
		{
			name: "test1",
			args: args{
				spans: []freespan{makeFreespan(4, 0)},
			},
			want: []freespan{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterZeroFreespanInPlace(tt.args.spans); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterZeroFreespanInPlace() = %v, want %v", got, tt.want)
			}
		})
	}
}
