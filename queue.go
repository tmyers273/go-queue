package queue

import (
	"fmt"
	"github.com/deckarep/golang-set"
	"go.uber.org/atomic"
	"time"
)

type Config struct {
	MaxWorkers int
}

type Job interface {
	Delay() time.Duration
	Work()
	ShouldWork() bool
	Key() string
}

type Queue struct {
	Config      Config
	WorkQueue   chan Job
	Workers     []Worker
	WorkerQueue chan chan Job
	Queued      mapset.Set

	//Empty    chan bool
	Done     chan bool
	QuitChan chan bool

	stopped atomic.Bool
}

func (q *Queue) Len() int {
	return q.Queued.Cardinality()
}

func (q *Queue) Start() {
	// First, initialize the channel we are going to but the workers' work channels into.
	q.WorkerQueue = make(chan chan Job, q.Config.MaxWorkers)
	q.WorkQueue = make(chan Job)
	q.Workers = make([]Worker, q.Config.MaxWorkers)
	//q.Empty = make(chan bool)
	q.Done = make(chan bool)
	q.QuitChan = make(chan bool)
	q.stopped.Store(false)

	q.Queued = mapset.NewSet()

	// Now, create all of our workers.
	fmt.Println("Starting", q.Config.MaxWorkers, "workers")
	for i := 0; i < q.Config.MaxWorkers; i++ {
		worker := NewWorker(i+1, q.WorkerQueue, q.Queued, q)
		worker.Start()
		q.Workers[i] = worker
	}
	fmt.Println("Done starting workers")

	go func() {
		for {
			select {
			case work := <-q.WorkQueue:
				go func() {
					worker := <-q.WorkerQueue

					worker <- work
				}()
			case <-q.QuitChan:
				for {
					if q.Queued.Cardinality() == 0 {
						fmt.Println("Closing", len(q.Workers), "workers")
						for _, w := range q.Workers {
							w.QuitChan <- true
						}
						close(q.WorkQueue)
						q.Done <- true
						break
					}
				}

				return
			}
		}
	}()
}

func (q *Queue) Stop() {
	q.stopped.Store(true)

	fmt.Println("Stopping queue")
	q.QuitChan <- true
	//for _, w := range q.Workers {
	//	w.Stop()
	//}
	//close(q.WorkQueue)
	//close(q.WorkerQueue)
}

func (q *Queue) Push(job Job) bool {
	key := job.Key()
	isRunning := q.Queued.Contains(key)

	if !isRunning && !q.stopped.Load() {
		q.Queued.Add(key)
		q.WorkQueue <- job
		return true
	}

	return false
}

// NewWorker creates, and returns a new Worker object. Its only argument
// is a channel that the worker can add itself to whenever it is done its
// work.
func NewWorker(id int, workerQueue chan chan Job, queued mapset.Set, queue *Queue) Worker {
	// Create, and return the worker.
	worker := Worker{
		ID:          id,
		Work:        make(chan Job),
		WorkerQueue: workerQueue,
		QuitChan:    make(chan bool),
		Queued:      queued,
		Queue:       queue,
		//Empty:       empty,
	}

	return worker
}

type Worker struct {
	ID          int
	Work        chan Job
	WorkerQueue chan chan Job
	QuitChan    chan bool
	Queued      mapset.Set
	Queue       *Queue
	//Empty       chan bool
}

// This function "starts" the worker by starting a goroutine, that is
// an infinite "for-select" loop.
func (w *Worker) Start() {
	go func() {
		for {
			// Add ourselves into the worker queue.
			w.WorkerQueue <- w.Work

			select {
			case job := <-w.Work:
				// Receive a work request.
				func() {
					fmt.Printf("%s [Worker %3d] Received job %s\n", time.Now(), w.ID, job.Key())

					// @todo
					//defer common.TrackTime(time.Now(), finish)

					if job.ShouldWork() {
						defer func(job Job, w *Worker) {
							w.Queued.Remove(job.Key())
							message := fmt.Sprintf("[Worker %3d] Finished job %s", w.ID, job.Key())
							fmt.Println(time.Now(), message)
						}(job, w)

						job.Work()
					} else {
						fmt.Printf("%s [Worker %3d] Job %s should not run. Putting back \n", time.Now(), w.ID, job.Key())
						go func(job Job, w *Worker) {
							time.Sleep(job.Delay())
							w.Queue.WorkQueue <- job
						}(job, w)
					}
				}()
			case <-w.QuitChan:
				// We have been asked to stop.
				fmt.Printf("%s [Worker %3d] Stopping\n", time.Now(), w.ID)
				return
			}
		}
	}()
}

// Stop tells the worker to stop listening for work requests.
//
// Note that the worker will only stop *after* it has finished its work.
func (w *Worker) Stop() {
	go func(w *Worker) {
		w.QuitChan <- true
	}(w)
}
