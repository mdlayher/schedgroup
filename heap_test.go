package schedgroup

import (
	"container/heap"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func Test_tasksHeap(t *testing.T) {
	newTask := func(d time.Duration) task {
		return task{
			// Static start time for consistency, no call function.
			Deadline: time.Unix(0, 0).Add(d * time.Second),
		}
	}

	tests := []struct {
		name     string
		in, want []task
	}{
		{
			name: "ordered",
			in: []task{
				newTask(1),
				newTask(2),
				newTask(3),
			},
			want: []task{
				newTask(1),
				newTask(2),
				newTask(3),
			},
		},
		{
			name: "unordered",
			in: []task{
				newTask(3),
				newTask(1),
				newTask(2),
			},
			want: []task{
				newTask(1),
				newTask(2),
				newTask(3),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Push and pop all tasks to verify the heap.Interface implementation.
			var tasks tasks
			for _, v := range tt.in {
				heap.Push(&tasks, v)
			}

			var got []task
			for tasks.Len() > 0 {
				got = append(got, heap.Pop(&tasks).(task))
			}

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("unexpected output tasks (-want +got):\n%s", diff)
			}
		})
	}
}
