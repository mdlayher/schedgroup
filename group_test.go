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
		leeway = 10 * time.Millisecond
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
			sg.Delay(time.Duration(i+1)*spread, func() {
				timeC <- time.Now()
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

	for i := 0; i < 5; i++ {
		sg.Schedule(time.Now(), func() {
			panic("should not be called")
		})
		time.Sleep(2 * time.Millisecond)
	}

	if err := sg.Wait(); err != context.Canceled {
		t.Fatalf("expected context canceled, but got: %v", err)
	}
}

func TestGroupSchedulePast(t *testing.T) {
	t.Parallel()

	sg := schedgroup.New(context.Background())

	const n = 2

	// Each task will signal on a channel when it is run.
	sigC := make(chan struct{}, n)
	signal := func() {
		sigC <- struct{}{}
	}

	// Any negative delay or time in the past will cause the task to be
	// scheduled immediately.
	sg.Delay(-1*time.Second, signal)
	sg.Schedule(time.Now().Add(-1*time.Second), signal)

	if err := sg.Wait(); err != nil {
		t.Fatalf("failed to wait: %v", err)
	}

	if diff := cmp.Diff(n, len(sigC)); diff != "" {
		t.Fatalf("unexpected number of tasks run (-want +got):\n%s", diff)
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
		sg.Delay(1*time.Millisecond, func() {
			waitC <- struct{}{}
			atomic.AddInt32(&count, 1)
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

func TestGroupWaitContextDeadlineExceeded(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	sg := schedgroup.New(ctx)

	// This task is scheduled now and should run.
	doneC := make(chan struct{})
	sg.Schedule(time.Now(), func() {
		close(doneC)
	})

	// This task is delayed and should not run.
	sg.Delay(1*time.Second, func() {
		panic("should not be called")
	})

	// Make sure the first task ran and then expect deadline exceeded.
	<-doneC
	if err := sg.Wait(); err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, but got: %v", err)
	}
}

func TestGroupWaitNoContext(t *testing.T) {
	t.Parallel()

	sg := schedgroup.New(context.Background())

	timer := time.AfterFunc(5*time.Second, func() {
		panic("took too long")
	})
	defer timer.Stop()

	// Make sure both tasks complete before Wait unblocks.
	doneC := make(chan struct{}, 2)
	done := func() {
		doneC <- struct{}{}
	}

	sg.Schedule(time.Now(), done)
	sg.Delay(50*time.Millisecond, done)

	<-doneC
	<-doneC

	if err := sg.Wait(); err != nil {
		t.Fatalf("failed to wait: %v", err)
	}

}

func TestGroupScheduleAfterWaitPanic(t *testing.T) {
	t.Parallel()

	sg := schedgroup.New(context.Background())
	if err := sg.Wait(); err != nil {
		t.Fatalf("failed to wait: %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("no panic occurred")
		}

		// Probably better than exporting the message.
		const want = "schedgroup: attempted to schedule task after Group.Wait was called"

		if diff := cmp.Diff(want, r); diff != "" {
			t.Fatalf("unexpected panic (-want +got):\n%s", diff)
		}
	}()

	sg.Schedule(time.Now(), func() {
		panic("should not be scheduled")
	})
}

func TestGroupDoubleWaitPanic(t *testing.T) {
	t.Parallel()

	sg := schedgroup.New(context.Background())
	if err := sg.Wait(); err != nil {
		t.Fatalf("failed to wait: %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("no panic occurred")
		}

		// Probably better than exporting the message.
		const want = "schedgroup: multiple calls to Group.Wait"

		if diff := cmp.Diff(want, r); diff != "" {
			t.Fatalf("unexpected panic (-want +got):\n%s", diff)
		}
	}()

	sg.Wait()
	panic("wait did not panic")
}

func TestGroupScheduleNoTasks(t *testing.T) {
	t.Parallel()

	// Ensure Groups that schedule no work do not hang, as was previously the
	// case between monitor and Wait.
	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	defer wg.Wait()

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < 1024; j++ {
				sg := schedgroup.New(context.Background())
				if err := sg.Wait(); err != nil {
					panicf("failed to wait: %v", err)
				}
			}
		}()
	}
}

func TestGroupWaitAfterScheduled(t *testing.T) {
	t.Parallel()

	sg := schedgroup.New(context.Background())

	// This job should done before Wait can be called due to the signal send
	// and the sleep.
	doneC := make(chan struct{}, 2)
	sg.Schedule(time.Now(), func() {
		doneC <- struct{}{}
	})

	<-doneC
	time.Sleep(100 * time.Millisecond)

	if err := sg.Wait(); err != nil {
		t.Fatalf("failed to wait: %v", err)
	}
}

// This example demonstrates typical use of a Group.
func ExampleGroup_wait() {
	// Create a Group which will not use a context for cancelation.
	sg := schedgroup.New(context.Background())

	// Schedule tasks to run in 100, 200, and 300 milliseconds which will print
	// the number n to the screen.
	for i := 0; i < 3; i++ {
		n := i + 1
		sg.Delay(time.Duration(n)*100*time.Millisecond, func() {
			fmt.Println(n)
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

// This example demonstrates how context cancelation/timeout effects a Group.
func ExampleGroup_cancelation() {
	// Create a Group which will use a context's timeout for cancelation.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	sg := schedgroup.New(ctx)

	// Schedule multiple tasks to occur at different times relative to a point
	// in time.
	start := time.Now()

	// Schedule a task which will not be run before a timeout occurs.
	sg.Schedule(start.Add(1*time.Second), func() {
		// This panic would normally crash the program, but this task will
		// never be run.
		panic("this shouldn't happen!")
	})

	// Schedule tasks which will occur before timeout. Tasks which are scheduled
	// for an earlier time will occur first.
	sg.Schedule(start.Add(200*time.Millisecond), func() {
		fmt.Println("world")
	})

	sg.Schedule(start.Add(100*time.Millisecond), func() {
		fmt.Println("hello")
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
