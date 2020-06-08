package fasthttp

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// BalancingClient is the interface for clients, which may be passed
// to LBClient.Clients.
type BalancingClient interface {
	DoDeadline(req *Request, resp *Response, deadline time.Time) error
	PendingRequests() int
}

type BalancingClientWeighted interface {
	BalancingClient
	SetWeight(weight float64)
	GetWeight() float64
}

type BalancingMode uint8

const (
	LeastLoaded    BalancingMode = iota
	RoundRobin     BalancingMode = iota
	WeightedRandom BalancingMode = iota
)

const DefaultBalancingMode = LeastLoaded

// LBClient balances requests among available LBClient.Clients.
//
// It has the following features:
//
//   - Balances load among available clients using 'least loaded' + 'least total'
//     hybrid technique.
//   - Dynamically decreases load on unhealthy clients.
//
// It is forbidden copying LBClient instances. Create new instances instead.
//
// It is safe calling LBClient methods from concurrently running goroutines.
type LBClient struct {
	noCopy noCopy //nolint:unused,structcheck

	// Clients must contain non-zero clients list.
	// Incoming requests are balanced among these clients.
	Clients []BalancingClient

	// HealthCheck is a callback called after each request.
	//
	// The request, response and the error returned by the client
	// is passed to HealthCheck, so the callback may determine whether
	// the client is healthy.
	//
	// Load on the current client is decreased if HealthCheck returns false.
	//
	// By default HealthCheck returns false if err != nil.
	HealthCheck func(req *Request, resp *Response, err error) bool

	// Timeout is the request timeout used when calling LBClient.Do.
	//
	// DefaultLBClientTimeout is used by default.
	Timeout time.Duration

	Mode BalancingMode

	RpsLimit int

	cs     []*lbClient
	csLock sync.RWMutex

	once sync.Once

	roundRobinCounter uint64
	weightSum         float64
}

// DefaultLBClientTimeout is the default request timeout used by LBClient
// when calling LBClient.Do.
//
// The timeout may be overridden via LBClient.Timeout.
const DefaultLBClientTimeout = time.Second

// DoDeadline calls DoDeadline on the least loaded client
func (cc *LBClient) DoDeadline(req *Request, resp *Response, deadline time.Time) error {
	return cc.get().DoDeadline(req, resp, deadline)
}

// DoTimeout calculates deadline and calls DoDeadline on the least loaded client
func (cc *LBClient) DoTimeout(req *Request, resp *Response, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	return cc.get().DoDeadline(req, resp, deadline)
}

// Do calls calculates deadline using LBClient.Timeout and calls DoDeadline
// on the least loaded client.
func (cc *LBClient) Do(req *Request, resp *Response) error {
	timeout := cc.Timeout
	if timeout <= 0 {
		timeout = DefaultLBClientTimeout
	}
	return cc.DoTimeout(req, resp, timeout)
}

func (cc *LBClient) init() {
	if len(cc.Clients) == 0 {
		panic("BUG: LBClient.Clients cannot be empty")
	}

	cc.ApplyClientsList()

	go cc.rpsResetWorker()
}

func (cc *LBClient) ApplyClientsList() {
	cc.csLock.Lock()
	defer cc.csLock.Unlock()

	cc.cs = cc.cs[:0]

	for _, c := range cc.Clients {
		lbc := &lbClient{
			c:           c,
			healthCheck: cc.HealthCheck,
		}
		cc.cs = append(cc.cs, lbc)

		if cc.Mode == WeightedRandom {
			if _, ok := c.(BalancingClientWeighted); !ok {
				panic("BUG: All clients should implement BalancingClientWeighted interface when using balancing mode weighted random")
			}

			lbc.weight = c.(BalancingClientWeighted).GetWeight()
		}
	}
}

func (cc *LBClient) RecalculateWeightSum() {
	var ws float64
	for _, c := range cc.Clients {
		if c, ok := c.(BalancingClientWeighted); ok {
			ws += c.GetWeight()
		}
	}
	cc.weightSum = ws
}

func (cc *LBClient) rpsResetWorker() {
	for range time.Tick(time.Second) {
		cc.csLock.RLock()

		for _, c := range cc.cs {
			atomic.StoreUint64(&c.rps, 0)
		}

		cc.csLock.RUnlock()
	}
}

func (cc *LBClient) getLeastLoaded() *lbClient {
	cc.csLock.RLock()
	defer cc.csLock.RUnlock()

	minC := cc.cs[0]
	minN := minC.PendingRequests()
	minT := atomic.LoadUint64(&minC.total)
	for _, c := range cc.cs[1:] {
		n := c.PendingRequests()
		t := atomic.LoadUint64(&c.total)
		if n < minN || (n == minN && t < minT) {
			minC = c
			minN = n
			minT = t
		}
	}
	return minC
}

func (cc *LBClient) getRoundRobin() *lbClient {
	cc.csLock.RLock()
	defer cc.csLock.RUnlock()

	return cc.cs[atomic.AddUint64(&cc.roundRobinCounter, 1)%uint64(len(cc.cs))]
}

func (cc *LBClient) getWeightSumLimited(limit int) (ws float64) {
	ws = cc.weightSum
	if limit == 0 {
		return
	}

	cc.csLock.RLock()
	defer cc.csLock.RUnlock()
	for _, c := range cc.cs {
		if atomic.LoadUint64(&c.rps) >= uint64(limit) {
			ws -= c.weight
		}
	}

	return
}

func (cc *LBClient) getWeightedRandom() *lbClient {
	r := rand.Float64() * cc.getWeightSumLimited(cc.RpsLimit)

	cc.csLock.RLock()
	defer cc.csLock.RUnlock()

	for _, c := range cc.cs {
		if cc.RpsLimit > 0 && atomic.LoadUint64(&c.rps) >= uint64(cc.RpsLimit) {
			continue
		}
		if r <= c.weight {
			if cc.RpsLimit > 0 {
				atomic.AddUint64(&c.rps, 1)
			}
			return c
		}
		r -= c.weight
	}

	return nil
}

func (cc *LBClient) get() *lbClient {
	cc.once.Do(cc.init)

	switch cc.Mode {
	case RoundRobin:
		return cc.getRoundRobin()
	case WeightedRandom:
		return cc.getWeightedRandom()
	case LeastLoaded:
		fallthrough
	default:
		return cc.getLeastLoaded()
	}
}

type lbClient struct {
	c           BalancingClient
	healthCheck func(req *Request, resp *Response, err error) bool
	penalty     uint32

	// total amount of requests handled.
	total  uint64
	rps    uint64
	weight float64
}

func (c *lbClient) DoDeadline(req *Request, resp *Response, deadline time.Time) error {
	err := c.c.DoDeadline(req, resp, deadline)
	if !c.isHealthy(req, resp, err) && c.incPenalty() {
		// Penalize the client returning error, so the next requests
		// are routed to another clients.
		time.AfterFunc(penaltyDuration, c.decPenalty)
	} else {
		atomic.AddUint64(&c.total, 1)
	}
	atomic.AddUint64(&c.rps, 1)
	return err
}

func (c *lbClient) PendingRequests() int {
	n := c.c.PendingRequests()
	m := atomic.LoadUint32(&c.penalty)
	return n + int(m)
}

func (c *lbClient) isHealthy(req *Request, resp *Response, err error) bool {
	if c.healthCheck == nil {
		return err == nil
	}
	return c.healthCheck(req, resp, err)
}

func (c *lbClient) incPenalty() bool {
	m := atomic.AddUint32(&c.penalty, 1)
	if m > maxPenalty {
		c.decPenalty()
		return false
	}
	return true
}

func (c *lbClient) decPenalty() {
	atomic.AddUint32(&c.penalty, ^uint32(0))
}

const (
	maxPenalty = 300

	penaltyDuration = 3 * time.Second
)
