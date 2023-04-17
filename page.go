package bbolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

// page header的大小，原版bolt还有额外的ptr字段，因此使用了
// int(unsafe.Offsetof(((*page)(nil)).ptr))这种更加Tricky的方法
const pageHeaderSize = unsafe.Sizeof(page{})

const minKeysPerPage = 2 // 每个page最少具有2个key

const branchPageElementSize = unsafe.Sizeof(branchPageElement{}) // b+tree inner-page中element的大小
const leafPageElementSize = unsafe.Sizeof(leafPageElement{})     // b+tree leaf-node中element的大小

const (
	branchPageFlag   = 0x01 // 表示page是一个inner-page
	leafPageFlag     = 0x02 // 表示page是一个leaf-page
	metaPageFlag     = 0x04 // 表示page是一个mate-page
	freelistPageFlag = 0x10 // 表示page是一个freelist-page
)

const (
	bucketLeafFlag = 0x01 // 表示leaf-page-element是一个嵌套的sub-bucket
)

type pgid uint64

// page header信息，每一个物理page的布局都是page header + data
type page struct {
	id       pgid   // page id
	flags    uint16 // page的类型，在上方有定义：branch page、leaf page、mate page、freelist page
	count    uint16 // 个数 2字节，统计叶子节点、非叶子节点、空闲列表页的个数
	overflow uint32 // 4字节，数据是否有溢出，主要在空闲列表上有用
}

// typ returns a human-readable page type string used for debugging.(返回类型string)
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// meta returns a pointer to the metadata section of the page.(返回元信息结构体指针，前提：是meta page)
func (p *page) meta() *meta {
	// 1.unsafe.Pointer(p)根据指针p的值(地址)，返回一个裸指针，仅代表一个地址，可以转换为指向任意类型的指针
	// 2.unsafe.Sizeof(*p)获取page类型的大小，相当于pageHeaderSize?
	// 3.unsafeAdd(base,offset)可以返回base+offset的地址，并将其转换为一个unsafe.Pointer
	// 4.将unsafe.Pointer转换为*meta返回
	return (*meta)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
}

// leafPageElement retrieves the leaf node by index(根据index返回对应的leaf-page-element指针)
func (p *page) leafPageElement(index uint16) *leafPageElement {
	// 1.unsafe.Pointer(p)根据指针p的值(地址)，返回一个裸指针，仅代表一个地址，可以转换为指向任意类型的指针
	// 2.unsafe.Sizeof(*p)获取page类型的大小，相当于pageHeaderSize?
	// 3.unsafeIndex(base,offset,elemsz,n)可以返回base+offset+n*elemsz的地址值，
	//	也就是page-header之后，第index个leaf-page-element的地址，并且转换为一个unsafe.Pointer
	// 4.将unsafe.Pointer转换为*leafPageElement返回
	return (*leafPageElement)(unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		leafPageElementSize, int(index)))
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	var elems []leafPageElement
	data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	unsafeSlice(unsafe.Pointer(&elems), data, int(p.count))
	return elems
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return (*branchPageElement)(unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		unsafe.Sizeof(branchPageElement{}), int(index)))
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	var elems []branchPageElement
	data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	unsafeSlice(unsafe.Pointer(&elems), data, int(p.count))
	return elems
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := unsafeByteSlice(unsafe.Pointer(p), 0, 0, n)
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
type branchPageElement struct {
	pos   uint32
	ksize uint32
	pgid  pgid
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	return unsafeByteSlice(unsafe.Pointer(n), 0, int(n.pos), int(n.pos)+int(n.ksize))
}

// leafPageElement represents a node on a leaf page.
type leafPageElement struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	i := int(n.pos)
	j := i + int(n.ksize)
	return unsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	i := int(n.pos) + int(n.ksize)
	j := i + int(n.vsize)
	return unsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}
