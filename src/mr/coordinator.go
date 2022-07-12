package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce    int
	mapAllDone bool
	// reduceNumber []bool

	// workerList      sync.Map // {WorkerNumber: doing taskName}
	mapTasksDone    sync.Map // {maptask: done}
	reduceTasksDone sync.Map // {reducetask: done}
	mapTasksQ       *TaskQueue
	reduceTasksQ    *TaskQueue

	// 输出文件
	muMapFiles        sync.Mutex
	muReduceFiles     sync.Mutex
	mapOutputFiles    []string
	reduceOutputFiles []string

	// 初始化reduce任务
	muInitReduce sync.Mutex

	// 处理超时任务
	timeoutTasks sync.Map
}

type WorkerType string
type WorkerNumber int

type Task interface {
}

type MapTask struct {
	name    string
	number  int
	nReduce int

	intputFile string

	startTime time.Time
}

type ReduceTask struct {
	name   string
	number int

	inputFile []string

	startTime time.Time
}

type TaskQueue struct {
	tasks *[]Task
	mu    sync.RWMutex
}

func (t *TaskQueue) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(*t.tasks)
}

func (t *TaskQueue) Push(x interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	*t.tasks = append(*t.tasks, x.(Task))
}

func (t *TaskQueue) Pop() interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	l := len(*t.tasks)
	if l <= 0 { // queue is empty
		return nil
	}
	popItem := (*t.tasks)[l-1]
	*t.tasks = (*t.tasks)[:l-1]
	return popItem
}

// Your code here -- RPC handlers for the worker to call.

// 因为测试是直接运行Worker, 不区分worker的类型, 由coordinator来管理分配任务就可以
// func (c *Coordinator) Register(args RegisterArgs, reply *RegisterReply) error {
// 	var workerID WorkerNumber = WorkerNumber(-1)
// 	var workerType WorkerType
// 	switch args.workerType {
// 	case WorkerType("Map"):
// 		retry := 0
// 		for {
// 			workerID = WorkerNumber(rand.Int())
// 			if _, ok := c.workerList.Load(workerID); !ok {
// 				break
// 			}
// 			retry++
// 			if retry >= 4096 {
// 				return fmt.Errorf("register failed")
// 			}
// 		}
// 	case WorkerType("Reduce"):
// 		for index, ok := range c.reduceNumber {
// 			if !ok {
// 				workerID = WorkerNumber(index)
// 				c.reduceNumber[index] = true
// 				break
// 			}
// 		}
// 		if workerID == WorkerNumber(-1) {
// 			return fmt.Errorf("register failed, reduceNumber all used")
// 		}
// 	default:
// 		return fmt.Errorf("taskType wrong")
// 	}

// 	c.workerList.Store(workerID, "")
// 	reply.workerNumber = workerID
// 	return nil
// }

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	taskType := "Map"
	c.muInitReduce.Lock()
	if c.mapAllDone || c.allMapTasksDone() { // 可能存在条件竞争, 同一时间只能一个协程来初始化reduce任务. allMapTasksDone只执行一次reduce任务的初始化.
		taskType = "Reduce"
	}
	c.muInitReduce.Unlock()

	switch taskType {
	case "Map":
		if c.mapTasksQ.Len() <= 0 {
			return fmt.Errorf("no map tasks")
		}
		// fmt.Printf("%p\n", c.mapTasksQ)
		task := (c.mapTasksQ.Pop()).(MapTask)
		task.startTime = time.Now()
		reply.WT = taskType
		reply.Name = task.name
		reply.Number = task.number
		reply.MapIntputFile = task.intputFile
		reply.NReduce = task.nReduce

		c.timeoutTasks.Store(task.number, &task)
		// fmt.Printf("%v\n", *reply)
		// fmt.Printf("%d\n", c.mapTasksQ.Len())
	case "Reduce":
		if c.reduceTasksQ.Len() <= 0 {
			return fmt.Errorf("no reduce tasks")
		}
		task := (c.reduceTasksQ.Pop()).(ReduceTask)
		task.startTime = time.Now()
		reply.WT = taskType
		reply.Name = task.name
		reply.Number = task.number
		reply.ReduceInputFile = task.inputFile

		c.timeoutTasks.Store(task.number, &task)
	default:
		return fmt.Errorf("taskType wrong")
	}

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	switch args.WT {
	case "Map":
		task := MapTask{
			name:       args.Name,
			number:     args.Number,
			intputFile: args.MapIntputFile,
			nReduce:    args.NReduce,
		}
		if args.Done { // 任务执行成功
			if v, ok := c.mapTasksDone.Load(task.number); ok { // 这个任务已经执行完成过
				if vb := v.(bool); vb { // 这个任务已经执行完成过
					return nil
				}
			} else {
				return fmt.Errorf("wrong task.number")
			}
			c.mapTasksDone.Store(task.number, true)
			c.timeoutTasks.Delete(task.number)
			c.muMapFiles.Lock()
			c.mapOutputFiles = append(c.mapOutputFiles, args.MapOutputFiles...)
			c.muMapFiles.Unlock()
		} else { // 执行失败
			c.mapTasksQ.Push(task)
		}
		reply.Done = true
	case "Reduce":
		task := ReduceTask{
			name:      args.Name,
			number:    args.Number,
			inputFile: args.ReduceInputFile,
		}
		if args.Done {
			if v, ok := c.reduceTasksDone.Load(task.number); ok { // 这个任务已经执行完成过
				if vb := v.(bool); vb { // 这个任务已经执行完成过
					return nil
				}
			} else {
				return fmt.Errorf("wrong task.number")
			}
			c.reduceTasksDone.Store(task.number, true)
			c.timeoutTasks.Delete(task.number)
			c.muReduceFiles.Lock()
			c.reduceOutputFiles = append(c.reduceOutputFiles, args.ReduceOutputFile)
			c.muReduceFiles.Unlock()
		} else {
			c.reduceTasksQ.Push(task)
		}
		reply.Done = true
	default:
		reply.Done = false
		return fmt.Errorf("wrong workerType")
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
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

func (c *Coordinator) serverByTCP() {
	err := RegisterService(c)
	if err != nil {
		log.Fatal("rpc register error:", err)
		return
	}
	listener, err := net.Listen("tcp", "0.0.0.0:"+fmt.Sprintf("%d", 12345))
	if err != nil {
		log.Fatal("listen error:", err)
		return
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatal("listen accept error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if c.mapTasksQ.Len() > 0 || c.reduceTasksQ.Len() > 0 {
		return false
	}

	mapDone := true
	reduceDone := true
	c.mapTasksDone.Range(func(key, value interface{}) bool {
		if value != true {
			mapDone = false
			return false
		}
		return true
	})
	if !mapDone {
		return false
	}

	c.reduceTasksDone.Range(func(key, value interface{}) bool {
		if value != true {
			reduceDone = false
			return false
		}
		return true
	})
	if !reduceDone {
		return false
	}

	return true
}

// 判断map任务是否全部完成
func (c *Coordinator) allMapTasksDone() bool {
	if c.mapTasksQ.Len() > 0 {
		return false
	}

	done := true
	c.mapTasksDone.Range(func(key, value interface{}) bool {
		if value != true {
			done = false
			return done
		}
		return true
	})
	if !done {
		return false
	}

	// 初始化reduce任务
	for index := 0; index < c.nReduce; index++ {
		inputFiles := make([]string, 0)
		reduceN := strconv.Itoa(index)
		c.muMapFiles.Lock()
		for _, fileName := range c.mapOutputFiles {
			if ok, _ := regexp.MatchString("mr-[0-9]+-"+reduceN, fileName); ok {
				inputFiles = append(inputFiles, fileName)
			}
		}
		c.muMapFiles.Unlock()
		c.reduceTasksQ.Push(ReduceTask{
			number:    index,
			name:      reduceN,
			inputFile: inputFiles,
		})
		// fmt.Printf("%d: %v\n", index, inputFiles)
		c.reduceTasksDone.Store(index, false)
	}
	c.mapAllDone = true
	return true
}

// 循环处理超时任务
func (c *Coordinator) handleTimeoutTask() {
	for {
		c.timeoutTasks.Range(func(key, value interface{}) bool { // key: task number; value: *Task
			now := time.Now()
			switch value := value.(type) {
			case *MapTask:
				if now.Sub(value.startTime).Seconds() > 10 { // map超时
					c.mapTasksQ.Push(*value)
				}
			case *ReduceTask:
				if now.Sub(value.startTime).Seconds() > 10 { // reduce超时
					c.reduceTasksQ.Push(*value)
				}
			default:
				log.Fatal("unknow task type")
			}
			return true
		})
		time.Sleep(time.Second)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:    nReduce,
		mapAllDone: false,
		// reduceNumber:      make([]bool, nReduce),
		// workerList:        sync.Map{},
		mapTasksDone:    sync.Map{},
		reduceTasksDone: sync.Map{},
		mapTasksQ: &TaskQueue{
			tasks: new([]Task),
			mu:    sync.RWMutex{},
		},
		reduceTasksQ: &TaskQueue{
			tasks: new([]Task),
			mu:    sync.RWMutex{},
		},
		mapOutputFiles:    make([]string, 0),
		reduceOutputFiles: make([]string, 0),
		timeoutTasks:      sync.Map{},
	}

	// Your code here.
	// 初始化map任务
	mapCount := 0
	for _, file := range files {
		c.mapTasksQ.Push(MapTask{
			number:     mapCount,
			name:       file,
			intputFile: file,
			nReduce:    nReduce,
		})
		c.mapTasksDone.Store(mapCount, false)
		mapCount++
	}

	// for c.mapTasksQ.Len() > 0 {
	// 	task := (c.mapTasksQ.Pop()).(MapTask)
	// 	fmt.Printf("%v\n", task)
	// }
	// println(c.mapTasksQ.Len())
	// c.server() // 测试用Unix Socket
	c.serverByTCP()
	go c.handleTimeoutTask()
	return &c
}
