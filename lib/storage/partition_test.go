package storage

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestPartitionConcurrentMergeAndQuery(t *testing.T) {
	pt := NewPartition()

	const numInitialParts = 10
	const pointsPerPart = 100
	expectedTotalPoints := int64(numInitialParts * pointsPerPart)

	for i := 0; i < numInitialParts; i++ {
		points := make([]Point, pointsPerPart)
		for j := 0; j < pointsPerPart; j++ {
			points[j] = Point{
				Timestamp: int64(i*pointsPerPart + j),
				Value:     float64(i*pointsPerPart + j),
			}
		}
		p := newPart(points)
		pt.AddPart(p)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	numReaders := 5
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			sq := NewSearchQuery(pt)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					points, err := sq.Execute()
					if err != nil {
						t.Errorf("Reader %d error: %v", readerID, err)
						return
					}
					if int64(len(points)) != expectedTotalPoints {
						t.Errorf("Reader %d: expected %d points, got %d", readerID, expectedTotalPoints, len(points))
						return
					}
				}
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(42))
		for {
			select {
			case <-ctx.Done():
				return
			default:
				pt.partsLock.RLock()
				currentParts := make([]*part, len(pt.parts))
				copy(currentParts, pt.parts)
				pt.partsLock.RUnlock()

				if len(currentParts) < 2 {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				idx1 := r.Intn(len(currentParts))
				idx2 := r.Intn(len(currentParts))
				if idx1 == idx2 {
					idx2 = (idx1 + 1) % len(currentParts)
				}
				p1 := currentParts[idx1]
				p2 := currentParts[idx2]

				var mergedPoints []Point
				mergedPoints = append(mergedPoints, p1.points...)
				mergedPoints = append(mergedPoints, p2.points...)

				newP := newPart(mergedPoints)
				pt.MergeParts([]*part{p1, p2}, newP)

				time.Sleep(time.Duration(r.Intn(5)) * time.Millisecond)
			}
		}
	}()

	wg.Wait()
}
