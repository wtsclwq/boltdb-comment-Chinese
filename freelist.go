package bbolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// txPending holds a list of pgids and corresponding allocation txns
// that are pending to be freed.
type txPending struct {
	ids              []pgid
	alloctx          []txid // txids allocating the ids
	lastReleaseBegin txid   // beginning txid of last matching releaseRange
}

// pidSet holds the set of starting pgids which have the same span size
type pidSet map[pgid]struct{}

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	// free-list的类型
	freelistType FreelistType // freelist type
	// 所有free-page-id
	ids []pgid // all free and available free page ids.
	// page id和事务id的映射
	allocs map[pgid]txid // mapping of txid that allocated a pgid.
	// 事务id 和 被其占用过的，即将被释放的page id集合的映射
	pending map[txid]*txPending // mapping of soon-to-be free page ids by tx.
	// 空闲page和即将空闲page的速查字典
	cache map[pgid]bool // fast lookup of all free and pending page ids.
	// 用来快速查找指定大小的连续空闲page： key是连续空闲page的跨度，比如连续10个空闲page,连续8个空闲page,value是具有key个连续页面的free-page-list的起始page id
	freemaps map[uint64]pidSet // key is the size of continuous pages(span), value is a set which contains the starting pgids of same size
	// key是连续空闲页面的起始page id,value是连续page的跨度大小
	forwardMap map[pgid]uint64 // key is start pgid, value is its span size
	// key是连续空闲页面的末尾page id,value是连续page的跨度大小
	backwardMap map[pgid]uint64 // key is end pgid, value is its span size
	// 申请n个连续的free-page，并将事务id和page-id的映射存储起来
	allocate func(txid txid, n int) pgid // the freelist allocate func
	// 返回free-page数量
	freeCount func() int // the function which gives you free page number
	// ids存储多个连续free-page的起始page id,将这些free-page合并到this
	mergeSpans func(ids pgids) // the mergeSpan func
	// 返回所有free-page-id的集合
	getFreePageIDs func() []pgid // get free pgids func
	// 读取一个page链表，并利用这个链表初始化当前freelist实例
	readIDs func(pgids []pgid) // readIDs func reads list of pages and init the freelist
}

// newFreelist returns an empty, initialized freelist.(创建一个空的，初始化的freelist内存对象)
func newFreelist(freelistType FreelistType) *freelist {
	f := &freelist{
		freelistType: freelistType,
		allocs:       make(map[pgid]txid),
		pending:      make(map[txid]*txPending),
		cache:        make(map[pgid]bool),
		freemaps:     make(map[uint64]pidSet),
		forwardMap:   make(map[pgid]uint64),
		backwardMap:  make(map[pgid]uint64),
	}

	if freelistType == FreelistMapType {
		f.allocate = f.hashmapAllocate
		f.freeCount = f.hashmapFreeCount
		f.mergeSpans = f.hashmapMergeSpans
		f.getFreePageIDs = f.hashmapGetFreePageIDs
		f.readIDs = f.hashmapReadIDs
	} else {
		f.allocate = f.arrayAllocate
		f.freeCount = f.arrayFreeCount
		f.mergeSpans = f.arrayMergeSpans
		f.getFreePageIDs = f.arrayGetFreePageIDs
		f.readIDs = f.arrayReadIDs
	}

	return f
}

// size returns the size of the page after serialization.(返回freelist-page的大小)
// freelist-page的布局：[page-header] + [free-id1,free-id2, ... ,free-idn]
func (f *freelist) size() int {
	n := f.count()
	// 如果数量大于65535（超过page.count的存储上限），那么第一个元素将被用于存储count,所以元素数量+1,
	// 此时布局变为[page-header] + [count = n] + [free-id1,free-id2, ... ,free-idn]
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return int(pageHeaderSize) + (int(unsafe.Sizeof(pgid(0))) * n)
}

// count returns count of pages on the freelist. (返回free-page-count + pending-page-count)
func (f *freelist) count() int {
	return f.freeCount() + f.pendingCount()
}

// arrayFreeCount returns count of free pages(array version) (array-base的free-page-count)
func (f *freelist) arrayFreeCount() int {
	return len(f.ids)
}

// pendingCount returns count of pending pages. (即将被释放的page-count)
func (f *freelist) pendingCount() int {
	var count int
	for _, txp := range f.pending {
		count += len(txp.ids)
	}
	return count
}

// copyAll copies a list of all free ids and all pending ids in one sorted list. (复制所有的free-page-id和pending-page-id到dst数组)
// f.count returns the minimum length required for dst.
func (f *freelist) copyAll(dst []pgid) {
	m := make(pgids, 0, f.pendingCount())
	for _, txp := range f.pending {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	// 合并free-page-ids和pending-page-ids到dst数组
	mergepgids(dst, f.getFreePageIDs(), m)
}

// arrayAllocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
// [5,6,7,  13,14,15,16,18,19,20,  31,32]
// 开始分配一段连续的n个页。其中返回值为起始page-id。如果无法分配，则返回0即可
func (f *freelist) arrayAllocate(txid txid, n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	// 双指针查找连续序列
	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// Reset initial page if this is not contiguous.
		// id - previd == 1 则说明initial到id这段是连续的，否则不连续
		if previd == 0 || id-previd != 1 {
			// 如果不连续了，就从id开始继续寻找足够长的连续page
			initial = id
		}

		// If we found a contiguous block then remove it and return it.
		// 找到了跨度为n的连续的page list，将其从ids中删除并且返回
		if (id-initial)+1 == pgid(n) {
			// If we're allocating off the beginning then take the fast path
			// and just adjust the existing slice. This will use extra memory
			// temporarily but append() in free() will realloc the slice
			// as is necessary.
			// 如果是前n个page,直接截取ids即可
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				// 如果不是前n个，那么需要提取(i-n，i]，然后整体移动后半部分，然后舍弃原本的后n个
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// Remove from the free cache.
			// 更新缓存速查表
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}
			// 记录起始page和事务id的映射关系
			f.allocs[initial] = txid
			return initial
		}

		previd = id
	}
	return 0
}

// 将被txid占用的pages释放，加入到freelist.pending中，用于事务结束时，解除对pages的占用，包括overflow pages
// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// Free page and all its overflow pages.
	// 先拿到txid对应的txPending指针
	txp := f.pending[txid]
	if txp == nil {
		txp = &txPending{}
		f.pending[txid] = txp
	}
	// 删除p.id和占用该page事务id的映射关系
	allocTxid, ok := f.allocs[p.id]
	if ok {
		delete(f.allocs, p.id)
	} else if (p.flags & freelistPageFlag) != 0 {
		// 如果f.allocs中没有存储的映射关系，说明这是个freelist page，freelist总是由前一个事务占用
		// Freelist is always allocated by prior tx.
		allocTxid = txid - 1
	}

	// 释放p以及p的overflow page
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// Verify that page is not already free.
		// 如果当前page已经free，说明逻辑出现错误，直接panic
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		// 将当前page放入txPending中，并且更新缓存速查表
		txp.ids = append(txp.ids, id)
		txp.alloctx = append(txp.alloctx, allocTxid)
		f.cache[id] = true
	}
}

// 将id<=txid的事务暂时存放在pending中的page，全都加入到freelist.ids中
// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, txp := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, txp.ids...)
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
}

// 将begin到end的事务占用的page加入到freelist.ids
// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
func (f *freelist) releaseRange(begin, end txid) {
	if begin > end {
		return
	}
	var m pgids
	for tid, txp := range f.pending {
		if tid < begin || tid > end {
			continue
		}
		// Don't recompute freed pages if ranges haven't updated.
		// 如果上一次释放txp的 begin 事务和本次相同，直接跳过
		if txp.lastReleaseBegin == begin {
			continue
		}
		for i := 0; i < len(txp.ids); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			m = append(m, txp.ids[i])
			txp.ids[i] = txp.ids[len(txp.ids)-1]
			txp.ids = txp.ids[:len(txp.ids)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		txp.lastReleaseBegin = begin
		if len(txp.ids) == 0 {
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
}

// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid txid) {
	// Remove page ids from cache.
	txp := f.pending[txid]
	if txp == nil {
		return
	}
	var m pgids
	for i, pgid := range txp.ids {
		delete(f.cache, pgid)
		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			// Pending free aborted; restore page back to alloc list.
			f.allocs[pgid] = tx
		} else {
			// Freed page was allocated by this txn; OK to throw away.
			m = append(m, pgid)
		}
	}
	// Remove pages from pending list and mark as free if allocated by txid.
	delete(f.pending, txid)
	f.mergeSpans(m)
}

// freed returns whether a given page is in the free list.
func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

// 从磁盘page p加载freelist到f
// read initializes the freelist from a freelist page.
func (f *freelist) read(p *page) {
	if (p.flags & freelistPageFlag) == 0 {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.id, p.typ()))
	}
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	var idx, count = 0, int(p.count)
	// 如果count==65535,说明data域第一个元素用来存储count
	if count == 0xFFFF {
		idx = 1
		c := *(*pgid)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
		count = int(c)
		if count < 0 {
			panic(fmt.Sprintf("leading element count %d overflows int", c))
		}
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.ids = nil
	} else {
		var ids []pgid
		// unsafeIndex(base,offset,elemsz,n)可以返回base+offset+n*elemsz的地址值，
		//也就是page-header之后，第idx个pgid的地址，并且转换为一个unsafe.Pointer
		data := unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p), unsafe.Sizeof(ids[0]), idx)
		// 在data指向的地址处构造一个长度为count的pgid slice ids
		unsafeSlice(unsafe.Pointer(&ids), data, count)

		// copy the ids, so we don't modify on the freelist page directly
		idsCopy := make([]pgid, count)
		copy(idsCopy, ids)
		// Make sure they're sorted.
		// 读出的时候排序ids, 事务release的时候排序pending，参考release()
		sort.Sort(pgids(idsCopy))

		f.readIDs(idsCopy)
	}
}

// arrayReadIDs initializes the freelist from a given list of ids.
func (f *freelist) arrayReadIDs(ids []pgid) {
	f.ids = ids
	f.reindex()
}

func (f *freelist) arrayGetFreePageIDs() []pgid {
	return f.ids
}

// 将空闲列表*freelist f转换成*page p，以备写入磁盘，注意所有的pending pages也被认为是free的，
// 因为crash时，事务来不及释放pending pages,但是这些pages已经不会再被使用了
// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	// 将p设置为freelist-page
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	// page.count是16位的，因此最多存储65535，如果count大于65535，就要让data域的第一个元素存储count
	// freelist-page的布局：[page-header] + [count] + [page-id1,page-id2...page-idn]
	l := f.count()
	if l == 0 {
		p.count = uint16(l)
	} else if l < 0xFFFF {
		p.count = uint16(l)
		var ids []pgid
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		unsafeSlice(unsafe.Pointer(&ids), data, l)
		f.copyAll(ids)
	} else {
		p.count = 0xFFFF
		var ids []pgid
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		unsafeSlice(unsafe.Pointer(&ids), data, l+1)
		// 第一个元素存储count
		ids[0] = pgid(l)
		f.copyAll(ids[1:])
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
func (f *freelist) reload(p *page) {
	f.read(p)

	// Build a cache of only pending pages.
	pcache := make(map[pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []pgid
	for _, id := range f.getFreePageIDs() {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

// noSyncReload reads the freelist from pgids and filters out pending items.
func (f *freelist) noSyncReload(pgids []pgid) {
	// Build a cache of only pending pages.
	pcache := make(map[pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []pgid
	for _, id := range pgids {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {
	ids := f.getFreePageIDs()
	f.cache = make(map[pgid]bool, len(ids))
	for _, id := range ids {
		f.cache[id] = true
	}
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			f.cache[pendingID] = true
		}
	}
}

// arrayMergeSpans try to merge list of pages(represented by pgids) with existing spans but using array
func (f *freelist) arrayMergeSpans(ids pgids) {
	sort.Sort(ids)
	f.ids = pgids(f.ids).merge(ids)
}
