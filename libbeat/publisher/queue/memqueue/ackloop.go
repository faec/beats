// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package memqueue

// ackLoop implements the brokers asynchronous ACK worker.
// Multiple concurrent ACKs from consecutive published batches will be batched up by the
// worker, to reduce the number of signals to return to the producer and the
// broker event loop.
// Producer ACKs are run in the ackLoop go-routine.
type ackLoop struct {
	broker *broker

	// A list of batches given to queue consumers,
	// used to maintain sequencing of event acknowledgements.
	pendingBatches batchList
}

func newACKLoop(broker *broker) *ackLoop {
	return &ackLoop{broker: broker}
}

func (l *ackLoop) run() {
	b := l.broker
	for {
		select {
		case <-b.ctx.Done():
			// The queue is shutting down.
			return

		case chanList := <-b.consumedChan:
			// New batches have been generated, add them to the pending list
			l.pendingBatches.concat(&chanList)

			// Subtlety: because runLoop delivers batches to consumedChan
			// asynchronously, it's possible that they were already acknowledged
			// before being added to pendingBatches, and in that case we should
			// advance our position immediately.
			l.maybeAdvanceBatchPosition()

		case acked := <-b.ackedChan:
			// A batch has been acknowledged. Mark it as done, remove its events
			// from the queue buffer, and check if the queue position can be
			// advanced.
			l.handleBatchACK(acked)
			l.maybeAdvanceBatchPosition()
		}
	}
}

// Collect all contiguous acknowledged batches from the start of the
// pending list.
func (l *ackLoop) collectACKed() batchList {
	ackedBatches := batchList{}

	for !l.pendingBatches.empty() {
		batch := l.pendingBatches.front()
		// This check is safe since only the ackLoop goroutine can modify
		// "done" after the batch is created.
		if !batch.done {
			break
		}
		ackedBatches.append(l.pendingBatches.pop())
	}

	return ackedBatches
}

func (l *ackLoop) maybeAdvanceBatchPosition() {
	ackedBatches := l.collectACKed()
	if ackedBatches.empty() {
		return
	}
	count := 0
	for batch := ackedBatches.front(); batch != nil; batch = batch.next {
		count += batch.count
	}

	if count > 0 {
		if callback := l.broker.ackCallback; callback != nil {
			callback(count)
		}

		// report acks to waiting clients
		l.processACK(ackedBatches, count)
	}

	for !ackedBatches.empty() {
		// Release finished batch structs into the shared memory pool
		releaseBatch(ackedBatches.pop())
	}

	// return final ACK to EventLoop, in order to clean up internal buffer
	l.broker.logger.Debug("ackloop: return ack to broker loop:", count)

	l.broker.logger.Debug("ackloop:  done send ack")
}

func (l *ackLoop) handleBatchACK(b *batch) {
	b.done = true
}

// Called by ackLoop. This function exists to decouple the work of collecting
// and running producer callbacks from logical deletion of the events, so
// input callbacks can't block the queue by occupying the runLoop goroutine.
func (l *ackLoop) processACK(lst batchList, N int) {
	ackCallbacks := []func(){}
	// First we traverse the entries we're about to remove, collecting any callbacks
	// we need to run.
	lst.reverse()
	for !lst.empty() {
		batch := lst.pop()

		// Traverse entries from last to first, so we can acknowledge the most recent
		// ones first and skip subsequent producer callbacks.
		for i := batch.count - 1; i >= 0; i-- {
			entry := batch.rawEntry(i)
			if entry.producer == nil {
				continue
			}

			if entry.producerID <= entry.producer.state.lastACK {
				// This index was already acknowledged on a previous iteration, skip.
				entry.producer = nil
				continue
			}
			producerState := entry.producer.state
			count := int(entry.producerID - producerState.lastACK)
			ackCallbacks = append(ackCallbacks, func() { producerState.cb(count) })
			entry.producer.state.lastACK = entry.producerID
			entry.producer = nil
		}
	}
	// Signal runLoop to delete the events
	l.broker.deleteChan <- N

	// The events have been removed; notify their listeners.
	for _, f := range ackCallbacks {
		f()
	}
}
