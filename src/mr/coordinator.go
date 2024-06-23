package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const maxTaskTime = 10 // seconds

type TaskStat struct {
	BeginTime int64
	FileName  string
	FileIndex int
	PartIndex int
	NReduce   int
	NFiles    int
}

type TaskQueue struct {
	queue chan TaskStat
	mutex sync.Mutex
}

func NewTaskQueue(len int) TaskQueue {
	return TaskQueue{
		queue: make(chan TaskStat, len),
	}
}

func (q *TaskQueue) Enqueue(task TaskStat) {
	q.queue <- task
}

func (q *TaskQueue) Dequeue() TaskStat {
	return <-q.queue
}

func (q *TaskQueue) IsEmpty() bool {
	return len(q.queue) == 0
}

func (q *TaskQueue) Size() int {
	return len(q.queue)
}

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

func (q *TaskQueue) RemoveMapTaskItem(FileIndex int) {
	q.mutex.Lock()
	newQueue := make(chan TaskStat, len(q.queue))

	size := len(q.queue)

	for i := 0; i < size; i++ {
		item := <-q.queue
		if item.FileIndex != FileIndex {
			newQueue <- item
		}
	}
	fmt.Printf("the current queue list len is %v\n", len(newQueue))
	//将新队列中的元素发回原始队列
	newQueueSize := len(newQueue)
	for i := 0; i < newQueueSize; i++ {
		q.queue <- <-newQueue
	}
	fmt.Printf("the current q.queue list len is %v\n", len(q.queue))
	q.mutex.Unlock()
}

func (q *TaskQueue) RemoveReduceTaskItem(PartIndex int) {
	q.mutex.Lock()
	newQueue := make(chan TaskStat, len(q.queue))

	size := len(q.queue)

	for i := 0; i < size; i++ {
		item := <-q.queue
		if item.PartIndex != PartIndex {
			newQueue <- item
		}
	}

	newQueueSize := len(newQueue)
	//将新队列中的元素发回原始队列
	for i := 0; i < newQueueSize; i++ {
		q.queue <- <-newQueue
	}
	q.mutex.Unlock()
}

type Coordinator struct {
	// Your definitions here.
	// 设置四个队列，代表map reduce任务等待时、运行时的队列
	mapTaskWaiting    TaskQueue
	mapTaskRunning    TaskQueue
	reduceTaskWaiting TaskQueue
	reduceTaskRunning TaskQueue

	isMapTaskDone bool
	isDone        bool

	nFile   int
	nReduce int
}

// 定义任务分配类型的枚举值
const (
	Task_Map = iota
	Task_Reduce
	Task_Wait
	Task_End
)

type TaskInfo struct {
	Tasktype  int
	BeginTime int64
	FileName  string
	FileIndex int
	PartIndex int
	NReduce   int
	NFiles    int
}

func (t *TaskStat) GenerateTaskInfo() TaskInfo {
	result := TaskInfo{}
	result.BeginTime = t.BeginTime
	result.FileName = t.FileName
	result.FileIndex = t.FileIndex
	result.PartIndex = t.PartIndex
	result.NReduce = t.NReduce
	result.NFiles = t.NFiles

	return result
}

func (t *TaskStat) SetNow() {
	t.BeginTime = getNowTimeSecond()
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) distributeReduce() {
	c.isMapTaskDone = true
	//初始化reduceTaskWaiting队列
	for i := 0; i < c.nReduce; i++ {
		q := TaskStat{}
		q.FileIndex = i
		q.PartIndex = i
		q.NFiles = c.nFile
		q.NReduce = c.nReduce
		c.reduceTaskWaiting.Enqueue(q)
	}
}

func (c *Coordinator) RemoveTimeOutMapTaskItem() {
	c.mapTaskRunning.mutex.Lock()
	newQueue := make(chan TaskStat, len(c.mapTaskRunning.queue))

	size := len(c.mapTaskRunning.queue)

	for i := 0; i < size; i++ {
		now := getNowTimeSecond()
		item := <-c.mapTaskRunning.queue
		if now-item.BeginTime < maxTaskTime {
			newQueue <- item
		} else {
			c.mapTaskWaiting.Enqueue(item)
		}

	}
	fmt.Printf("the current queue list len is %v\n", len(newQueue))
	//将新队列中的元素发回原始队列
	newQueueSize := len(newQueue)
	for i := 0; i < newQueueSize; i++ {
		c.mapTaskRunning.queue <- <-newQueue
	}
	fmt.Printf("the current q.queue list len is %v\n", len(c.mapTaskRunning.queue))
	c.mapTaskRunning.mutex.Unlock()
}

func (c *Coordinator) RemoveTimeOutReduceTaskItem() {
	c.reduceTaskRunning.mutex.Lock()
	newQueue := make(chan TaskStat, len(c.reduceTaskRunning.queue))

	size := len(c.reduceTaskRunning.queue)

	for i := 0; i < size; i++ {
		now := getNowTimeSecond()
		item := <-c.reduceTaskRunning.queue
		if now-item.BeginTime < maxTaskTime {
			newQueue <- item
		} else {
			c.reduceTaskWaiting.Enqueue(item)
		}
	}

	newQueueSize := len(newQueue)
	//将新队列中的元素发回原始队列
	for i := 0; i < newQueueSize; i++ {
		c.reduceTaskRunning.queue <- <-newQueue
	}
	c.reduceTaskRunning.mutex.Unlock()
}

func (c *Coordinator) getDistributeReduceType() bool {
	return c.isMapTaskDone
}

func (c *Coordinator) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	if c.mapTaskWaiting.Size() != 0 {
		//分配map任务
		task := c.mapTaskWaiting.Dequeue()
		task.SetNow()
		c.mapTaskRunning.Enqueue(task)
		fmt.Printf("input : Map task list num %v\n", c.mapTaskRunning.Size())
		*reply = task.GenerateTaskInfo()
		reply.Tasktype = Task_Map
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}
	if c.reduceTaskWaiting.Size() != 0 && c.getDistributeReduceType() {
		//分配reduce任务
		task := c.reduceTaskWaiting.Dequeue()
		task.SetNow()
		c.reduceTaskRunning.Enqueue(task)
		*reply = task.GenerateTaskInfo()
		reply.Tasktype = Task_Reduce
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n", reply.PartIndex, reply.FileIndex, reply.FileName)
		return nil
	}
	//map 和 reduce任务都分配完了 查看是否还有运行的任务
	if c.reduceTaskRunning.Size() != 0 || c.mapTaskRunning.Size() != 0 || !c.getDistributeReduceType() {
		reply.Tasktype = Task_Wait
		return nil
	}

	reply.Tasktype = Task_End
	c.isDone = true
	return nil
}

func (c *Coordinator) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.Tasktype {
	case Task_Map:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		//这边可能需要根据指定item进行删除
		c.mapTaskRunning.RemoveMapTaskItem(args.FileIndex)
		//要在map任务运行完成后才执行后续的reduce任务
		fmt.Printf("Out : Map task list num %v\n", c.mapTaskRunning.Size())
		if c.mapTaskRunning.Size() == 0 && c.mapTaskWaiting.Size() == 0 {
			c.distributeReduce()
		}
		break
	case Task_Reduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		c.reduceTaskRunning.RemoveReduceTaskItem(args.PartIndex)
		break
	default:
		panic("Task Done error")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.isDone

	return ret
}

func (c *Coordinator) removeTimeOutTask() {
	c.RemoveTimeOutMapTaskItem()
	c.RemoveTimeOutReduceTaskItem()

}
func (c *Coordinator) loopRemoveTimeOut() {
	for true {
		time.Sleep(2 * 1000 * time.Millisecond)
		c.removeTimeOutTask()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.isDone = false
	c.isMapTaskDone = false

	c.mapTaskWaiting = NewTaskQueue(len(files))
	c.mapTaskRunning = NewTaskQueue(len(files))
	c.reduceTaskRunning = NewTaskQueue(nReduce)
	c.reduceTaskWaiting = NewTaskQueue(nReduce)

	log.Printf("the file len is %v\n", len(files))
	// Your code here.
	c.nFile = len(files)
	c.nReduce = nReduce

	//start a thread to handle the timeout task
	go c.loopRemoveTimeOut()

	for i := 0; i < len(files); i++ {
		q := TaskStat{}
		q.FileName = files[i]
		q.FileIndex = i
		q.NReduce = nReduce
		log.Printf("input taskstat FileName %v\n", q.FileName)
		c.mapTaskWaiting.Enqueue(q)
	}

	c.server()
	return &c
}
