package fasthttp

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// Dial dials the given TCP addr using tcp4.
//
// This function has the following additional features comparing to net.Dial:
//
//   * It reduces load on DNS resolver by caching resolved TCP addressed
//     for DefaultDNSCacheDuration.
//   * It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   * It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialTimeout for customizing dial timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//     * foobar.baz:443
//     * foo.bar:80
//     * aaa.com:8080
func Dial(addr string) (net.Conn, error) {
	return defaultDialer.Dial(addr)
}

// DialTimeout dials the given TCP addr using tcp4 using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   * It reduces load on DNS resolver by caching resolved TCP addressed
//     for DefaultDNSCacheDuration.
//   * It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//     * foobar.baz:443
//     * foo.bar:80
//     * aaa.com:8080
func DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return defaultDialer.DialTimeout(addr, timeout)
}

// DialDualStack dials the given TCP addr using both tcp4 and tcp6.
//
// This function has the following additional features comparing to net.Dial:
//
//   * It reduces load on DNS resolver by caching resolved TCP addressed
//     for DefaultDNSCacheDuration.
//   * It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   * It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialDualStackTimeout for custom dial
//     timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//     * foobar.baz:443
//     * foo.bar:80
//     * aaa.com:8080
func DialDualStack(addr string) (net.Conn, error) {
	return defaultDialer.DialDualStack(addr)
}

// DialDualStackTimeout dials the given TCP addr using both tcp4 and tcp6
// using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   * It reduces load on DNS resolver by caching resolved TCP addressed
//     for DefaultDNSCacheDuration.
//   * It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//     * foobar.baz:443
//     * foo.bar:80
//     * aaa.com:8080
func DialDualStackTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return defaultDialer.DialDualStackTimeout(addr, timeout)
}

var (
	defaultDialer = &TCPDialer{Concurrency: 1000}
)

// Resolver represents interface of the tcp resolver.
type Resolver interface {
	LookupIPAddr(context.Context, string) (names []net.IPAddr, err error)
}

type TCPResolver interface {
	LookupTCPAddr(context.Context, string) (addrs []net.TCPAddr, err error)
}

// TCPDialer contains options to control a group of Dial calls.
type TCPDialer struct {
	// Concurrency controls the maximum number of concurrent Dails
	// that can be performed using this object.
	// Setting this to 0 means unlimited.
	//
	// WARNING: This can only be changed before the first Dial.
	// Changes made after the first Dial will not affect anything.
	Concurrency int

	// LocalAddr is the local address to use when dialing an
	// address.
	// If nil, a local address is automatically chosen.
	LocalAddr *net.TCPAddr

	// This may be used to override DNS resolving policy, like this:
	// var dialer = &fasthttp.TCPDialer{
	// 	Resolver: &net.Resolver{
	// 		PreferGo:     true,
	// 		StrictErrors: false,
	// 		Dial: func (ctx context.Context, network, address string) (net.Conn, error) {
	// 			d := net.Dialer{}
	// 			return d.DialContext(ctx, "udp", "8.8.8.8:53")
	// 		},
	// 	},
	// }
	Resolver TCPResolver

	concurrencyCh chan struct{}

	once sync.Once
}

// Dial dials the given TCP addr using tcp4.
//
// This function has the following additional features comparing to net.Dial:
//
//   * It reduces load on DNS resolver by caching resolved TCP addressed
//     for DefaultDNSCacheDuration.
//   * It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   * It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialTimeout for customizing dial timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//     * foobar.baz:443
//     * foo.bar:80
//     * aaa.com:8080
func (d *TCPDialer) Dial(addr string) (net.Conn, error) {
	return d.dial(addr, false, DefaultDialTimeout)
}

// DialTimeout dials the given TCP addr using tcp4 using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   * It reduces load on DNS resolver by caching resolved TCP addressed
//     for DefaultDNSCacheDuration.
//   * It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//     * foobar.baz:443
//     * foo.bar:80
//     * aaa.com:8080
func (d *TCPDialer) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return d.dial(addr, false, timeout)
}

// DialDualStack dials the given TCP addr using both tcp4 and tcp6.
//
// This function has the following additional features comparing to net.Dial:
//
//   * It reduces load on DNS resolver by caching resolved TCP addressed
//     for DefaultDNSCacheDuration.
//   * It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   * It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialDualStackTimeout for custom dial
//     timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//     * foobar.baz:443
//     * foo.bar:80
//     * aaa.com:8080
func (d *TCPDialer) DialDualStack(addr string) (net.Conn, error) {
	return d.dial(addr, true, DefaultDialTimeout)
}

// DialDualStackTimeout dials the given TCP addr using both tcp4 and tcp6
// using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   * It reduces load on DNS resolver by caching resolved TCP addressed
//     for DefaultDNSCacheDuration.
//   * It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//     * foobar.baz:443
//     * foo.bar:80
//     * aaa.com:8080
func (d *TCPDialer) DialDualStackTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return d.dial(addr, true, timeout)
}

func (d *TCPDialer) dial(addr string, dualStack bool, timeout time.Duration) (net.Conn, error) {
	d.once.Do(func() {
		if d.Concurrency > 0 {
			d.concurrencyCh = make(chan struct{}, d.Concurrency)
		}
	})

	resolver := d.Resolver
	if resolver == nil {
		resolver = defaultTCPResolver
	}

	addrs, err := resolver.LookupTCPAddr(context.Background(), addr)
	if err != nil {
		return nil, err
	}

	if len(addrs) == 0 {
		return nil, errNoDNSEntries
	}

	network := "tcp4"
	if dualStack {
		network = "tcp"
	}

	var conn net.Conn
	deadline := time.Now().Add(timeout)
	for n := range addrs {
		if !dualStack && addrs[n].IP.To4() == nil {
			continue
		}

		conn, err = d.tryDial(network, &addrs[n], deadline, d.concurrencyCh)
		if err == nil {
			return conn, nil
		}
		if err == ErrDialTimeout {
			return nil, err
		}
	}

	if err == nil {
		err = errNoDNSEntries
	}

	return nil, err
}

func (d *TCPDialer) tryDial(network string, addr *net.TCPAddr, deadline time.Time, concurrencyCh chan struct{}) (net.Conn, error) {
	timeout := -time.Since(deadline)
	if timeout <= 0 {
		return nil, ErrDialTimeout
	}

	if concurrencyCh != nil {
		select {
		case concurrencyCh <- struct{}{}:
		default:
			tc := AcquireTimer(timeout)
			isTimeout := false
			select {
			case concurrencyCh <- struct{}{}:
			case <-tc.C:
				isTimeout = true
			}
			ReleaseTimer(tc)
			if isTimeout {
				return nil, ErrDialTimeout
			}
		}
	}

	chv := dialResultChanPool.Get()
	if chv == nil {
		chv = make(chan dialResult, 1)
	}
	ch := chv.(chan dialResult)
	go func() {
		var dr dialResult
		dr.conn, dr.err = net.DialTCP(network, d.LocalAddr, addr)
		ch <- dr
		if concurrencyCh != nil {
			<-concurrencyCh
		}
	}()

	var (
		conn net.Conn
		err  error
	)

	tc := AcquireTimer(timeout)
	select {
	case dr := <-ch:
		conn = dr.conn
		err = dr.err
		dialResultChanPool.Put(ch)
	case <-tc.C:
		err = ErrDialTimeout
	}
	ReleaseTimer(tc)

	return conn, err
}

var dialResultChanPool sync.Pool

type dialResult struct {
	conn net.Conn
	err  error
}

// ErrDialTimeout is returned when TCP dialing is timed out.
var ErrDialTimeout = errors.New("dialing to the given TCP address timed out")

// DefaultDialTimeout is timeout used by Dial and DialDualStack
// for establishing TCP connections.
const DefaultDialTimeout = 3 * time.Second

var errNoDNSEntries = errors.New("couldn't find DNS entries for the given domain. Try using DialDualStack")

var defaultTCPResolver = NewTCPResolverCached(DefaultTCPResolverCacheTTL, true)