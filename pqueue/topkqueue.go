package pqueue

// TopKQueue maintains a fixed-size queue of items
// with k highest priorities.
type TopKQueue struct {
	PQueue
	k int
}

func NewTopKQueue(k int) *TopKQueue {
	return &TopKQueue{
		*NewPQueue(MINPQ),
		k,
	}
}

// DryPush checks whether a Push with the given priority
// will result in a materialized insertion to the
// TopKQueue
func (pq *TopKQueue) DryPush(priority float64) bool {
	if pq.Size() < pq.k {
		return true
	}
	_, bottom := pq.Head()
	if bottom < priority {
		return true
	}
	return false
}

// Push pushes a new item to the TopKQueue, but does not
// actually insert the item into the queue unless its
// priority qualifies for the top-k
func (pq *TopKQueue) Push(value interface{}, priority float64) {
	if !pq.DryPush(priority) {
		return
	}
	if pq.Size() == pq.k {
		pq.Pop()
	}
	pq.PQueue.Push(value, priority)
}
