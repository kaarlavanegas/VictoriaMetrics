package storage

import (
	"fmt"
	"sync"
)

type Partition struct {
	partsLock sync.RWMutex
	parts     []*part
}

func NewPartition() *Partition {
	return &Partition{}
}

func (pt *Partition) AddPart(p *part) {
	pt.partsLock.Lock()
	defer pt.partsLock.Unlock()
	p.incRef()
	pt.parts = append(pt.parts, p)
}

func (pt *Partition) GetPartsForSearch() ([]*part, error) { 
	pt.partsLock.RLock()
	defer pt.partsLock.RUnlock()

	parts := make([]*part, 0, len(pt.parts))
	for _, p := range pt.parts {
		if p.incRef() {
			parts = append(parts, p)
		} else {
			for _, ap := range parts {
				ap.decRef()
			}
			return nil, fmt.Errorf("found closed part in active parts list")
		}
	}
	return parts, nil
}

func (pt *Partition) MergeParts(srcParts []*part, newPart *part) {
	pt.partsLock.Lock()
	
	newParts := make([]*part, 0, len(pt.parts)-len(srcParts)+1)
	if newPart != nil {
		newPart.incRef()
		newParts = append(newParts, newPart)
	}

	srcMap := make(map[*part]bool)
	for _, p := range srcParts {
		srcMap[p] = true
	}

	for _, p := range pt.parts {
		if !srcMap[p] {
			newParts = append(newParts, p)
		}
	}

	pt.parts = newParts
	pt.partsLock.Unlock()

	for _, p := range srcParts {
		p.decRef()
	}
}
