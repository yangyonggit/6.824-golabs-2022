package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	inputFileMap map[string]int //value 0 未派发，1已经派发任务,2表示完成
	nReduce      int
	reduceTask   []int //false 未执行1已经派发任务,2表示完成
	mu           sync.Mutex
	allWorkDone  int

	mapTaskClocks    map[string]time.Time
	reduceTaskClocks []time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *WorkerArgs, task *WorkTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	waitForMapWork := false
	for filename, isSend := range c.inputFileMap {
		if isSend != 2 {
			waitForMapWork = true
		}

		if isSend == 0 {
			task.IsAllTaskFinished = false
			task.Wait = false
			task.IsMapTask = true
			task.Filename = filename
			task.NReduce = c.nReduce
			task.IndexReduce = -1
			c.inputFileMap[filename] = 1
			c.mapTaskClocks[filename] = time.Now()
			// fmt.Println("Patch map task .... %v", task.IsMapTask)
			return nil
		}
	}

	if waitForMapWork {
		task.IsAllTaskFinished = false
		task.Wait = true
		// fmt.Println("Tell worker wait ....")
		return nil
	}

	for i, v := range c.reduceTask {
		if v == 0 {
			task.IsAllTaskFinished = false
			task.Wait = false
			task.IsMapTask = false
			task.Filename = ""
			task.NReduce = c.nReduce
			task.IndexReduce = i
			c.reduceTask[i] = 1
			c.reduceTaskClocks[i] = time.Now()
			// fmt.Println("Patch reduce work .... %v", task.IsMapTask)
			return nil
		}
	}
	// fmt.Println("Patch all work ....")
	task.IsAllTaskFinished = false
	task.Wait = true
	return nil
}

func (c *Coordinator) ReciveMapWorkDone(args *MapWorkArgs, reply *MapWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inputFileMap[args.Filename] = 2
	// fmt.Println("ReciveMapWorkDone %v", args.Filename)
	return nil
}

func (c *Coordinator) ReciveReduceWordDone(args *ReduceWorkArgs, reply *ReduceWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTask[args.Index] = 2
	c.allWorkDone++
	// fmt.Println("ReciveReduceWordDone %v ----  %v", args.Index, c.allWorkDone)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	for k, v := range c.mapTaskClocks {
		if c.inputFileMap[k] == 1 && time.Now().Sub(v) > time.Second*10 {
			// fmt.Println("Map task  over time %v", k)
			c.inputFileMap[k] = 0
		}
	}

	for k, v := range c.reduceTaskClocks {
		if c.reduceTask[k] == 1 && time.Now().Sub(v) > time.Second*10 {
			// fmt.Println("R task  over time %v", k)
			c.reduceTask[k] = 0
		}
	}

	if c.allWorkDone == c.nReduce {
		// fmt.Println("All Task Done....")
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inputFileMap = make(map[string]int)
	c.reduceTask = make([]int, nReduce)
	c.mapTaskClocks = make(map[string]time.Time)
	c.reduceTaskClocks = make([]time.Time, nReduce)

	for _, filename := range files {
		c.inputFileMap[filename] = 0
	}
	c.nReduce = nReduce
	for i := 0; i < c.nReduce; i++ {
		c.reduceTask[i] = 0
	}
	c.allWorkDone = 0

	c.server()
	return &c
}
