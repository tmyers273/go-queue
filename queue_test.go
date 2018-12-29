package queue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

type TestJob struct {
	Name  string
	value int
}

func (t *TestJob) Delay() time.Duration {
	return 1 * time.Second
}

func (t *TestJob) ShouldWork() bool {
	should := t.value%2 == 0

	if !should {
		t.value++
	}

	return should
}

func (t *TestJob) Work() {
	fmt.Println(time.Now(), "["+t.Key()+"]", "Starting work")
	time.Sleep(1 * time.Second)
	t.value++

	fmt.Println(time.Now(), "["+t.Key()+"]", " Done working")
}

func (t *TestJob) Key() string {
	return t.Name
}

//func TestQueue_Start(t *testing.T) {
//	queue := Queue{
//		Config: Config{
//			MaxWorkers: 5,
//		},
//	}
//	queue.Start()
//
//	jobCount := 10
//	jobs := makeJobs(jobCount)
//
//	for i := 0; i < jobCount; i++ {
//		go func(i int) {
//			queue.Push(&jobs[i])
//		}(i)
//	}
//
//	//empty := <-queue.Empty
//	//assert.True(t, empty)
//}

func TestQueue_Stop(t *testing.T) {
	queue := Queue{
		Config: Config{
			MaxWorkers: 5,
		},
	}
	queue.Start()

	jobCount := 10
	jobs := makeJobs(jobCount)

	failCount := 0
	for i := 0; i < jobCount; i++ {
		pushed := queue.Push(&jobs[i])
		if i == 5 {
			queue.Stop()
		}

		if !pushed {
			failCount++
		}
	}

	//queue.Push(&jobs[0])
	//time.Sleep(3 * time.Second)
	//queue.Push(&jobs[0])
	//time.Sleep(3 * time.Second)

	//<-queue.Empty
	<-queue.Done
	//time.Sleep(6 * time.Second)
	assert.True(t, failCount > 0)
}

func TestQueue_cant_push_duplicate(t *testing.T) {
	// 1. Given
	queue := Queue{
		Config: Config{
			MaxWorkers: 5,
		},
	}
	queue.Start()

	jobCount := 10
	jobs := makeJobs(jobCount)

	// 2. Do this
	push1 := queue.Push(&jobs[0])
	push2 := queue.Push(&jobs[1])
	push3 := queue.Push(&jobs[0])

	// 3. Expect
	assert.True(t, push1)
	assert.True(t, push2)
	assert.False(t, push3)
}

func TestQueue_puts_back(t *testing.T) {
	// 1. Given
	queue := Queue{
		Config: Config{
			MaxWorkers: 5,
		},
	}
	queue.Start()

	jobCount := 10
	jobs := makeJobs(jobCount)

	// 2. Do this
	push1 := queue.Push(&jobs[0])
	time.Sleep(1200 * time.Millisecond)
	push3 := queue.Push(&jobs[0])
	queue.Stop()
	time.Sleep(3200 * time.Millisecond)

	// 3. Expect
	assert.True(t, push1)
	assert.True(t, push3)
}

func makeJobs(count int) []TestJob {
	jobs := make([]TestJob, count)
	for i := 0; i < count; i++ {
		jobs[i] = TestJob{
			Name: strconv.Itoa(i),
		}
	}
	return jobs
}
