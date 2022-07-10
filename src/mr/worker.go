package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := WorkerArgs{}

		task := WorkTask{}
		allDone := Request(&args, &task)
		if allDone {
			return
		}

		if task.Wait {
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		if task.IsMapTask {
			if doMapTask(mapf, &task) {
				ReportMapWorkDone(&task)
			}
		} else {
			if doReduceTask(reducef, &task) {
				ReportReduceWorkDone(&task)
			}
		}

	}
}

func Request(args *WorkerArgs, task *WorkTask) bool {

	ok := call("Coordinator.RequestTask", &args, &task)
	if !ok {
		// fmt.Printf("Connect corrdinator failed.Worker exit.\n")
		return true
	}

	return task.IsAllTaskFinished
}

func ReportMapWorkDone(task *WorkTask) {
	args := MapWorkArgs{}
	args.Filename = task.Filename

	reply := MapWorkReply{}

	call("Coordinator.ReciveMapWorkDone", &args, &reply)
}

func ReportReduceWorkDone(task *WorkTask) {
	args := ReduceWorkArgs{}
	args.Index = task.IndexReduce

	reply := ReduceWorkReply{}

	call("Coordinator.ReciveReduceWordDone", &args, &reply)
}

func doMapTask(mapf func(string, string) []KeyValue, task *WorkTask) bool {
	// fmt.Println("doMapTask " + task.Filename)
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
		return false
	}
	file.Close()
	kva := mapf(task.Filename, string(content))

	nReduce := task.NReduce
	resultList := make([]ByKey, nReduce)

	for _, v := range kva {
		resultList[ihash(v.Key)%nReduce] = append(resultList[ihash(v.Key)%nReduce], v)
	}

	for _, list := range resultList {
		sort.Sort(ByKey(list))
	}

	for i, list := range resultList {
		oname := "tmp-mr-mapresult-" + strconv.Itoa(i) + "-" + filepath.Base(task.Filename)
		fname := "mr-mapresult-" + strconv.Itoa(i) + "-" + filepath.Base(task.Filename)
		// fmt.Println("oname %v", oname)
		ofile, err := ioutil.TempFile(".", oname)

		if err != nil {
			log.Fatalf("Create file failed %v  %v", oname, err)
			return false
		}
		for _, v := range list {
			fmt.Fprintf(ofile, "%v %v\n", v.Key, v.Value)
		}
		ofile.Close()
		ferr := os.Rename(ofile.Name(), fname)
		if ferr != nil {
			return false
		}
	}

	return true
}

func doReduceTask(reducef func(string, []string) string, task *WorkTask) bool {
	// fmt.Println("doReduceTask " + strconv.Itoa(task.IndexReduce))
	var fileList []string
	files, _ := ioutil.ReadDir(".")
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-mapresult-"+strconv.Itoa(task.IndexReduce)) {
			// fmt.Println("Find file %v", file.Name())
			fileList = append(fileList, file.Name())
		}
	}

	// var resultMap map[string]([]string)
	var allkv []KeyValue
	for _, fileName := range fileList {
		file, err := os.Open(fileName)
		if err != nil {
			// fmt.Println("Can't open file " + fileName)
			return false
		}

		scanner := bufio.NewScanner(file)
		// optionally, resize scanner's capacity for lines over 64K, see next example

		for scanner.Scan() {
			kv := KeyValue{}
			fmt.Sscanf(scanner.Text(), "%v %v\n", &kv.Key, &kv.Value)
			allkv = append(allkv, kv)
		}
	}

	sort.Sort(ByKey(allkv))

	oname := "tmp-mr-out" + strconv.Itoa(task.IndexReduce)
	finalName := "mr-out" + strconv.Itoa(task.IndexReduce)
	ofile, _ := ioutil.TempFile(".", oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(allkv) {
		j := i + 1
		for j < len(allkv) && allkv[j].Key == allkv[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allkv[k].Value)
		}
		output := reducef(allkv[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", allkv[i].Key, output)

		i = j
	}

	ofile.Close()
	err := os.Rename(ofile.Name(), finalName)

	return err == nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
