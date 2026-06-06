package storage

import (
	"fmt"
)

type SearchQuery struct {
	pt *Partition
}

func NewSearchQuery(pt *Partition) *SearchQuery {
	return &SearchQuery{pt: pt}
}

func (sq *SearchQuery) Execute() ([]Point, error) {
	parts, err := sq.pt.GetPartsForSearch()
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, p := range parts {
			p.decRef()
		}
	}()

	var results []Point
	for _, p := range parts {
		if p.isClosed() {
			return nil, fmt.Errorf("attempted to read from a closed part")
		}
		results = append(results, p.points...)
	}

	return results, nil
}
