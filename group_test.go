// Copyright 2020 Matt Layher
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedgroup_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/mdlayher/schedgroup"
)

func TestGroupScheduling(t *testing.T) {
	t.Parallel()

	sg := schedgroup.New(context.Background())

	// Schedule N tasks that should be roughly spread duration apart, with a
	// bit of leeway in each direction.
	const (
		n      = 5
		spread = 100 * time.Millisecond
		leeway = 5 * time.Millisecond
	)

	timeC := make(chan time.Time, n)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer func() {
			close(timeC)
			wg.Done()
		}()

		// Produce the current time when a task is fired.
		for i := 0; i < n; i++ {
			sg.Delay(time.Duration(i+1)*spread, func() error {
				timeC <- time.Now()
				return nil
			})
		}

		if err := sg.Wait(); err != nil {
			panicf("failed to wait: %v", err)
		}
	}()

	var (
		last time.Time
		recv int
	)

	for tv := range timeC {
		recv++

		if !last.IsZero() {
			diff := tv.Sub(last)

			// Assume that each task should have been scheduled roughly spread
			// seconds apart, with some leeway.
			if diff < spread-leeway || diff > spread+leeway {
				t.Fatalf("expected roughly %s +/- %s difference, but got: %v", spread, leeway, diff)
			}
		}

		last = tv
	}

	if diff := cmp.Diff(n, recv); diff != "" {
		t.Fatalf("unexpected number of received values (-want +got):\n%s", diff)
	}
}

func TestGroupContextCancelImmediate(t *testing.T) {
	t.Parallel()

	// Context canceled before the Group is created, so no tasks should ever run.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sg := schedgroup.New(ctx)

	sg.Schedule(time.Now(), func() error {
		panic("should not be called")
	})

	if err := sg.Wait(); err != context.Canceled {
		t.Fatalf("expected context canceled, but got: %v", err)
	}
}

func TestGroupScheduledTasksContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sg := schedgroup.New(ctx)

	// Assume we want to process work repeatedly but eventually our caller
	// wants no more tasks to be scheduled.
	waitC := make(chan struct{})
	var count int32
	for i := 0; i < 10; i++ {
		sg.Delay(1*time.Millisecond, func() error {
			waitC <- struct{}{}
			atomic.AddInt32(&count, 1)
			return nil
		})

		// Blocks until closed halfway through. Any further sends will result
		// in a panic, failing the test.
		<-waitC

		if i == 5 {
			cancel()
			close(waitC)
		}
	}

	if err := sg.Wait(); err != context.Canceled {
		t.Fatalf("expected context canceled, but got: %v", err)
	}

	if diff := cmp.Diff(6, int(atomic.LoadInt32(&count))); diff != "" {
		t.Fatalf("unexpected number of tasks scheduled (-want +got):\n%s", diff)
	}
}

func TestGroupScheduledTasksDeadlineExceeded(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	sg := schedgroup.New(ctx)

	// This task is scheduled now and should run.
	doneC := make(chan struct{})
	sg.Schedule(time.Now(), func() error {
		close(doneC)
		return nil
	})

	// This task is delayed and should not run.
	sg.Delay(1*time.Second, func() error {
		panic("should not be called")
	})

	// Make sure the first task ran and then expect deadline exceeded.
	<-doneC
	if err := sg.Wait(); err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, but got: %v", err)
	}
}

func ExampleGroup_wait() {
	// Create a Group which will not use a context for cancelation.
	sg := schedgroup.New(context.Background())

	// Schedule tasks to run in 1, 2, and 3 seconds which will print the number
	// n to the screen.
	for i := 0; i < 3; i++ {
		n := i + 1
		sg.Delay(time.Duration(n)*100*time.Millisecond, func() error {
			fmt.Println(n)
			return nil
		})
	}

	// Wait for all of the scheduled tasks to complete.
	if err := sg.Wait(); err != nil {
		log.Fatalf("failed to wait: %v", err)
	}

	// Output:
	// 1
	// 2
	// 3
}

func ExampleGroup_cancelation() {
	// Create a Group which will use a context's timeout for cancelation.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	sg := schedgroup.New(ctx)

	// Schedule multiple tasks to occur at different times relative to a point
	// in time.
	start := time.Now()

	sg.Schedule(start.Add(100*time.Millisecond), func() error {
		fmt.Println("hello")
		return nil
	})

	sg.Schedule(start.Add(200*time.Millisecond), func() error {
		fmt.Println("world")
		return nil
	})

	// Schedule a task which will not be run before a timeout occurs.
	sg.Schedule(start.Add(1*time.Second), func() error {
		// This panic would normally crash the program, but this task will
		// never be run.
		panic("this shouldn't happen!")
	})

	// Wait for task completion or timeout.
	switch err := sg.Wait(); err {
	case nil:
		panic("all tasks should not have completed!")
	case context.DeadlineExceeded:
		// No problem, we expected this to occur.
		fmt.Println("timeout!")
	default:
		log.Fatalf("failed to wait: %v", err)
	}

	// Output:
	// hello
	// world
	// timeout!
}

func panicf(format string, a ...interface{}) {
	panic(fmt.Sprintf(format, a...))
}
