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

package fifo

type FIFO[T any] struct {
	first *node[T]
	last  *node[T]
}

type node[T any] struct {
	next  *node[T]
	value T
}

func (f *FIFO[T]) Add(value T) {
	newNode := &node[T]{value: value}
	if f.first == nil {
		f.first = newNode
	} else {
		f.last.next = newNode
	}
	f.last = newNode
}

func (f *FIFO[T]) Empty() bool {
	return f.first == nil
}

// Return the first value (if present) without removing it from the queue.
// Returns a default value if the queue is empty. To recognize this case,
// check (*FIFO).Empty().
func (f *FIFO[T]) First() T {
	if f.first == nil {
		var none T
		return none
	}
	return f.first.value
}

// Remove the first entry in this FIFO and return it.
func (f *FIFO[T]) ConsumeFirst() T {
	result := f.First()
	f.Remove()
	return result
}

// Append another FIFO queue to an existing one. Takes ownership of
// the given FIFO's contents.
func (f *FIFO[T]) Concat(list FIFO[T]) {
	if list.Empty() {
		return
	}
	if f.Empty() {
		*f = list
		return
	}
	f.last.next = list.first
	f.last = list.last
}

// Remove the first entry in the queue. Does nothing if the FIFO is empty.
func (f *FIFO[T]) Remove() {
	if f.first != nil {
		f.first = f.first.next
		if f.first == nil {
			f.last = nil
		}
	}
}
