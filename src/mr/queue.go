package mr

import (
	"errors"
)

type Queue []uint64

func (queue *Queue) Len() int {
	return len(*queue)
}

func (queue *Queue) IsEmpty() bool {
	return len(*queue) == 0
}

func (queue *Queue) Cap() int {
	return cap(*queue)
}

func (queue *Queue) Push(value uint64) {
	*queue = append(*queue, value)
}

func (queue *Queue) Pop() (uint64, error) {
	theQueue := *queue
	if len(theQueue) == 0 {
		return 0, errors.New("Out of index, len is 0")
	}
	value := theQueue[0]
	*queue = theQueue[1:len(theQueue)]
	return value, nil
}

func (queue *Queue) Index(i uint64) uint64 {
	theQueue := *queue
	return theQueue[i]
}

