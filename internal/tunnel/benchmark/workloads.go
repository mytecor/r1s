package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"
)

// --- benchmark rows (shared methodology) ------------------------------------

// runOpenLatency measures per-connection establishment cost: the time to open a
// fresh logical connection (one Link on RNS, one pair+preamble on Ygg) and
// exchange a single byte. This is the direct "overhead of the RNS Link path"
// the feature records.
func openLatency(ctx context.Context, c Connector, iters int) (time.Duration, error) {
	var total time.Duration
	for i := 0; i < iters; i++ {
		elapsed, err := openLatencyOnce(ctx, c)
		if err != nil {
			return 0, err
		}
		total += elapsed
	}
	return total, nil
}

func openLatencyOnce(ctx context.Context, c Connector) (time.Duration, error) {
	client, allocator, err := c.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	defer allocator.Close()
	stopInterrupt := interruptOnCancel(ctx, client, allocator)
	defer stopInterrupt()

	start := time.Now()
	// A single byte request/ack verifies both directions are live.
	if _, err := client.Write([]byte{0x01}); err != nil {
		return 0, fmt.Errorf("open ping write: %w", err)
	}
	if _, err := readAll(allocator, 1); err != nil {
		return 0, fmt.Errorf("open ping read: %w", err)
	}
	if _, err := allocator.Write([]byte{0x02}); err != nil {
		return 0, fmt.Errorf("open pong write: %w", err)
	}
	if _, err := readAll(client, 1); err != nil {
		return 0, fmt.Errorf("open pong read: %w", err)
	}
	return time.Since(start), nil
}

// runHTTPLatency measures one small request/response round-trip over an already
// established connection: the interactive-protocol latency (HTTP headers, SSH
// handshake) the relay must not inflate. Each Open is reused across iters so
// this isolates steady-state per-message latency from per-connection setup.
func runHTTPLatency(ctx context.Context, c Connector, iters int) (time.Duration, error) {
	client, allocator, err := c.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	defer allocator.Close()
	stopInterrupt := interruptOnCancel(ctx, client, allocator)
	defer stopInterrupt()

	request := make([]byte, 512)   // ~ an HTTP GET + a few headers
	response := make([]byte, 2048) // ~ a small HTTP response body
	if _, err := rand.Read(request); err != nil {
		return 0, err
	}
	if _, err := rand.Read(response); err != nil {
		return 0, err
	}

	var total time.Duration
	for i := 0; i < iters; i++ {
		start := time.Now()
		if err := writeAll(client, request); err != nil {
			return 0, fmt.Errorf("req write: %w", err)
		}
		if _, err := readAll(allocator, len(request)); err != nil {
			return 0, fmt.Errorf("req read: %w", err)
		}
		if err := writeAll(allocator, response); err != nil {
			return 0, fmt.Errorf("resp write: %w", err)
		}
		if _, err := readAll(client, len(response)); err != nil {
			return 0, fmt.Errorf("resp read: %w", err)
		}
		total += time.Since(start)
	}
	return total, nil
}

// runConcurrent measures n connections transferring concurrently. The driver
// opens all n, then times n simultaneous full transfers. This is where the
// transport maps n connections to its primitive: n streams on one Ygg pair, or
// n independent Links on RNS. A slow transport under concurrency shows here.
func runConcurrent(ctx context.Context, c Connector, n, size int) (time.Duration, error) {
	conns := make([]struct {
		client, allocator Conn
	}, n)
	for i := 0; i < n; i++ {
		client, allocator, err := c.Open(ctx)
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].client.Close()
				conns[j].allocator.Close()
			}
			return 0, fmt.Errorf("open conn %d: %w", i, err)
		}
		conns[i].client, conns[i].allocator = client, allocator
	}
	defer func() {
		for i := range conns {
			conns[i].client.Close()
			conns[i].allocator.Close()
		}
	}()
	allConns := make([]Conn, 0, 2*len(conns))
	for i := range conns {
		allConns = append(allConns, conns[i].client, conns[i].allocator)
	}
	stopInterrupt := interruptOnCancel(ctx, allConns...)
	defer stopInterrupt()

	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		return 0, err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	start := time.Now()
	for i := range conns {
		wg.Add(1)
		go func(client, allocator Conn) {
			defer wg.Done()
			recvDone := make(chan error, 1)
			go func() {
				_, err := readAll(allocator, size)
				recvDone <- err
			}()
			if err := writeAll(client, payload); err != nil {
				errCh <- err
				return
			}
			if err := client.CloseWrite(); err != nil {
				errCh <- err
				return
			}
			if err := <-recvDone; err != nil && err != io.EOF {
				errCh <- err
			}
		}(conns[i].client, conns[i].allocator)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errCh)
	for err := range errCh {
		if err != nil {
			return 0, err
		}
	}
	return elapsed, nil
}
