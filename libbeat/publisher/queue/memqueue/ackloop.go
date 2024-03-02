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

	callbackWorker callbackWorker
}

type callbackWorker struct {
	callbackChan  chan []func()
	callbacksDone chan struct{}
}

func newACKLoop(broker *broker) *ackLoop {
	return &ackLoop{
		broker: broker,
		callbackWorker: callbackWorker{
			callbackChan:  make(chan []func(), 5),
			callbacksDone: make(chan struct{}),
		},
	}
}

func (cw *callbackWorker) run() {
	pendingCallbacks := [][]func(){}
	callbackInProgress := false
	for {
		select {
		case newCallbacks, ok := <-cw.callbackChan:
			if !ok {
				// channel has been closed, shutting down
				return
			}
			if !callbackInProgress {
				callbackInProgress = true
				go func() {
					for _, cb := range newCallbacks {
						cb()
					}
					cw.callbacksDone <- struct{}{}
				}()
			} else {
				pendingCallbacks = append(pendingCallbacks, newCallbacks)
			}
		case <-cw.callbacksDone:
			if len(pendingCallbacks) > 0 {
				nextCallbacks := pendingCallbacks[0]
				copy(pendingCallbacks, pendingCallbacks[1:])
				pendingCallbacks = pendingCallbacks[:len(pendingCallbacks)-1]
				go func() {
					for _, cb := range nextCallbacks {
						cb()
					}
					cw.callbacksDone <- struct{}{}
				}()
			} else {
				callbackInProgress = false
			}
		}
	}
}

func (l *ackLoop) run() {
	go l.callbackWorker.run()
	b := l.broker
	for {
		nextBatchChan := l.pendingBatches.nextBatchChannel()

		select {
		case <-b.ctx.Done():
			// The queue is shutting down.
			close(l.callbackWorker.callbackChan)
			return

		case chanList := <-b.consumedChan:
			// New batches have been generated, add them to the pending list
			l.pendingBatches.concat(&chanList)

		case <-nextBatchChan:
			// The oldest outstanding batch has been acknowledged, advance our
			// position as much as we can.
			l.handleBatchSig()
		}
	}
}

// handleBatchSig collects and handles a batch ACK/Cancel signal. handleBatchSig
// is run by the ackLoop.
func (l *ackLoop) handleBatchSig() int {
	ackedBatches := l.collectAcked()

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
	return count
}

func (l *ackLoop) collectAcked() batchList {
	ackedBatches := batchList{}

	acks := l.pendingBatches.pop()
	ackedBatches.append(acks)

	done := false
	for !l.pendingBatches.empty() && !done {
		acks := l.pendingBatches.front()
		select {
		case <-acks.doneChan:
			ackedBatches.append(l.pendingBatches.pop())

		default:
			done = true
		}
	}

	return ackedBatches
}

// Called by ackLoop. This function exists to decouple the work of collecting
// and running producer callbacks from logical deletion of the events, so
// input callbacks can't block the queue by occupying the runLoop goroutine.
func (l *ackLoop) processACK(lst batchList, N int) {
	batchCount := 0
	ackCallbacks := []func(){}
	// First we traverse the entries we're about to remove, collecting any callbacks
	// we need to run.
	lst.reverse()
	for !lst.empty() {
		batch := lst.pop()
		batchCount++

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
			//producerState := entry.producer.state
			oldLastACK := entry.producer.state.lastACK
			cb := entry.producer.state.cb
			count := int(entry.producerID - oldLastACK)
			ackCallbacks = append(ackCallbacks, func() { cb(count) })
			entry.producer.state.lastACK = entry.producerID
			entry.producer = nil
		}
	}
	// Signal runLoop to delete the events
	l.broker.deleteChan <- N

	// If we just freed multiple batches, make sure the input is unblocked
	if batchCount > 1 {
		l.broker.unblockCPU()
	}

	// The events have been removed; notify their listeners.
	l.callbackWorker.callbackChan <- ackCallbacks
}
