package storage

import (
	"sync/atomic"
)

type Point struct {
	Timestamp int64
	Value     float64
}

type part struct {
	refCount int32
	points   []Point
	closed   int32
}

func newPart(points []Point) *part {
	return &part{
		refCount: 1,
		points:   points,
	}
}

func (p *part) incRef() bool {
	for {
		rc := atomic.LoadInt32(&p.refCount)
		if rc <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(&p.refCount, rc, rc+1) {
			return true
		}
	}
}

func (p *part) decRef() {
	if atomic.AddInt32(&p.refCount, -1) == 0 {
		atomic.StoreInt32(&p.closed, 1)
		p.points = nil
	}
}

func (p *part) isClosed() bool {
	return atomic.LoadInt32(&p.closed) == 1
}
