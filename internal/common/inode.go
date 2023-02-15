package common

// Inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
type Inode struct {
	flags uint32
	pgid  Pgid
	key   []byte
	value []byte
}

type Inodes []Inode

func (in *Inode) Flags() uint32 {
	return in.flags
}

func (in *Inode) SetFlags(flags uint32) {
	in.flags = flags
}

func (in *Inode) Pgid() Pgid {
	return in.pgid
}

func (in *Inode) SetPgid(id Pgid) {
	in.pgid = id
}

func (in *Inode) Key() []byte {
	return in.key
}

func (in *Inode) SetKey(key []byte) {
	in.key = key
}

func (in *Inode) Value() []byte {
	return in.value
}

func (in *Inode) SetValue(value []byte) {
	in.value = value
}
