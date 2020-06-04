package fasthttp

import (
	"context"
	"github.com/cespare/xxhash/v2"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const DefaultTCPResolverCacheTTL = time.Minute

type TCPResolverCached struct {
	cache       []map[string]*tcpAddrItem
	cacheLock   []sync.RWMutex
	updateQueue chan string
	ipResolver  Resolver
	curTime     time.Time
	cacheTTL    time.Duration
	ipv4Only    bool
}

type tcpAddrItem struct {
	addrs       []net.TCPAddr
	resolveTime time.Time
}

func NewTCPResolverCached(cacheTTL time.Duration, ipv4Only bool) (r *TCPResolverCached) {
	r = new(TCPResolverCached)

	r.ipResolver = net.DefaultResolver
	r.curTime = time.Now()
	r.cacheTTL = cacheTTL
	if r.cacheTTL == 0 {
		r.cacheTTL = DefaultTCPResolverCacheTTL
	}
	r.ipv4Only = ipv4Only

	procs := runtime.GOMAXPROCS(0)
	shards := procs * 10

	r.cache = make([]map[string]*tcpAddrItem, shards)
	r.cacheLock = make([]sync.RWMutex, shards)

	r.updateQueue = make(chan string, procs*100)

	for i := range r.cache {
		r.cache[i] = make(map[string]*tcpAddrItem)
	}

	go r.updateCacheWorker()

	go func() {
		for r.curTime = range time.Tick(time.Second) {
		}
	}()

	return
}

func (r *TCPResolverCached) updateCacheWorker() {
	for addr := range r.updateQueue {
		_ = r.resolveTCPAddr(context.Background(), addr)
	}
}

func (r *TCPResolverCached) resolveTCPAddr(ctx context.Context, addr string) (err error) {
	addrHash := xxhash.Sum64String(addr)
	shard := addrHash % uint64(len(r.cache))
	var timeDiff time.Duration

	r.cacheLock[shard].Lock()
	defer r.cacheLock[shard].Unlock()

	addrItem, ok := r.cache[shard][addr]
	if ok {
		timeDiff = r.curTime.Sub(addrItem.resolveTime)
	} else {
		addrItem = new(tcpAddrItem)
		r.cache[shard][addr] = addrItem
	}

	ok = ok && timeDiff < r.cacheTTL

	if ok {
		return
	}

	host, strPort, err := net.SplitHostPort(addr)
	if err != nil {
		return
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return
	}

	ipAddrs, err := r.ipResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return
	}

	capDiff := len(ipAddrs) - cap(addrItem.addrs)

	if capDiff > 0 {
		addrItem.addrs = append(addrItem.addrs, make([]net.TCPAddr, capDiff)...)
	}

	addrItem.addrs = addrItem.addrs[:len(ipAddrs)]

	cacheItemNum := 0
	for ipAddrNum := 0; ipAddrNum < len(ipAddrs); ipAddrNum++ {
		if r.ipv4Only && ipAddrs[ipAddrNum].IP.To4() == nil {
			continue
		}

		addrItem.addrs[cacheItemNum].IP = ipAddrs[ipAddrNum].IP
		addrItem.addrs[cacheItemNum].Zone = ipAddrs[ipAddrNum].Zone
		addrItem.addrs[cacheItemNum].Port = port

		addrItem.resolveTime = r.curTime

		cacheItemNum++
	}

	addrItem.addrs = addrItem.addrs[:cacheItemNum]

	return
}

func (r *TCPResolverCached) LookupTCPAddr(ctx context.Context, addr string) (addrs []net.TCPAddr, err error) {
	addrHash := xxhash.Sum64String(addr)
	shard := addrHash % uint64(len(r.cache))
	var timeDiff time.Duration

	r.cacheLock[shard].RLock()
	addrItem, ok := r.cache[shard][addr]
	if ok {
		timeDiff = r.curTime.Sub(addrItem.resolveTime)
	}
	r.cacheLock[shard].RUnlock()

	ok = ok && timeDiff < r.cacheTTL*2

	if ok {
		if timeDiff > r.cacheTTL {
			r.updateQueue <- addr
		}

		addrs = addrItem.addrs

		return
	}

	err = r.resolveTCPAddr(ctx, addr)
	if err != nil {
		return
	}

	r.cacheLock[shard].RLock()
	defer r.cacheLock[shard].RUnlock()
	addrs = r.cache[shard][addr].addrs

	return
}
