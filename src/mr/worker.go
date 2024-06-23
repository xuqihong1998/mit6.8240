package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CallTaskDone(taskInfo *TaskInfo) {
	reply := ExampleReply{}
	call("Coordinator.TaskDone", taskInfo, &reply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		taskInfo := CallAskTask()
		log.Printf("taskInfo receive, the fileIndex is %v\n", taskInfo.FileIndex)
		switch taskInfo.Tasktype {
		case Task_Map:
			outprefix := "../mr-tmp/mr-" + strconv.Itoa(taskInfo.FileIndex) + "/mr-"
			intermediate := []KeyValue{}
			file, err := os.Open(taskInfo.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", taskInfo.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", taskInfo.FileName)
			}
			file.Close()
			kva := mapf(taskInfo.FileName, string(content))
			intermediate = append(intermediate, kva...)
			//定义nReduce 创建输出文件
			nReduce := taskInfo.NReduce
			outFiles := make([]*os.File, nReduce)
			fileEncs := make([]*json.Encoder, nReduce)

			// 创建临时目录
			tmpDir := "../mr-tmp/mr-" + strconv.Itoa(taskInfo.FileIndex)
			err = os.MkdirAll(tmpDir, 0755)
			if err != nil {
				fmt.Printf("Failed to create temp directory: %v\n", err)
				return
			}

			for outindex := 0; outindex < nReduce; outindex++ {
				outdir := "../mr-tmp/mr-" + strconv.Itoa(taskInfo.FileIndex)
				outFiles[outindex], err = os.CreateTemp(outdir, "mr-tmp-*")
				fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
				if err != nil {
					fmt.Printf("Failed to create temp file: %v\n", err)
				}
			}
			//将kv输入本地暂时文件
			for _, kv := range intermediate {
				outindex := ihash(kv.Key) % nReduce
				file = outFiles[outindex]
				enc := fileEncs[outindex]
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Printf("File %v Key %v value %v Error: %v\n", taskInfo.FileName, kv.Key, kv.Value, err)
					panic("Json encode failed")
				}
			}

			//保存文件
			for outindex, file := range outFiles {
				outname := outprefix + strconv.Itoa(outindex)
				oldpath := filepath.Join(file.Name())
				os.Rename(oldpath, outname)
				file.Close()
			}

			taskInfo.Tasktype = Task_Map

			CallTaskDone(taskInfo)
			break
		case Task_Reduce:
			//to do
			outname := fmt.Sprintf("mr-out-%v", taskInfo.PartIndex)

			innameprefix := "../mr-tmp/mr-"
			//innamesuffix := "-" + strconv.Itoa(taskInfo.PartIndex)
			//读取文件为键值对
			intermediate := []KeyValue{}
			for index := 0; index < taskInfo.NFiles; index++ {
				inname := innameprefix + strconv.Itoa(index) + "/mr-" + strconv.Itoa(taskInfo.PartIndex)
				file, err := os.Open(inname)
				if err != nil {
					fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
					panic("Open file error")
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			ofile, err := os.CreateTemp("../mr-tmp", "mr-*")
			if err != nil {
				fmt.Printf("Create output file failed: %v\n", err)
				panic("Create file error")
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// 输出文件
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			os.Rename(filepath.Join(ofile.Name()), outname)
			ofile.Close()
			taskInfo.Tasktype = Task_Reduce
			CallTaskDone(taskInfo)

			break
		case Task_Wait:
			time.Sleep(time.Duration(time.Second * 5))
			break
		case Task_End:
			fmt.Println("Master all tasks complete. Nothing to do...")
			return
		default:
			panic("Invalid Task state received by worker")
		}

	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func CallAskTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		log.Printf("call success, the fileName is %v, the task type is %v\n", reply.FileName, reply.Tasktype)
	} else {
		log.Printf("call failed!\n")
	}

	return &reply
}
