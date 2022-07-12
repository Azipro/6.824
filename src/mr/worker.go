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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		if !callByTCP("GetTask", &args, &reply) { // 获取任务失败或者暂时没有任务
			time.Sleep(time.Second * 3)
			continue
		}
		switch reply.WT {
		case "Map":
			// go func(args GetTaskArgs, reply GetTaskReply) { // 同时两个rpc连接，会有冲突，后一个连接会重置前一个连接
			outputFiles, err := Map(reply.Number, reply.NReduce, reply.MapIntputFile, mapf)
			args1 := FinishTaskArgs{
				WT:            "Map",
				Name:          reply.Name,
				Number:        reply.Number,
				MapIntputFile: reply.MapIntputFile,
				NReduce:       reply.NReduce,
			}
			reply1 := FinishTaskReply{}
			if err != nil {
				args1.Done = false
				fmt.Printf("map task failed [error]: %v\n", err)
			} else {
				args1.MapOutputFiles = outputFiles
				args1.Done = true
			}

			if !callByTCP("FinishTask", &args1, &reply1) && !reply1.Done {
				fmt.Printf("map task failed\n")
			}
			// }(args, reply)
		case "Reduce":
			// go func(args GetTaskArgs, reply GetTaskReply) {
			outputFile, err := Reduce(reply.Number, reply.ReduceInputFile, reducef)
			args1 := FinishTaskArgs{
				WT:              "Reduce",
				Name:            reply.Name,
				Number:          reply.Number,
				ReduceInputFile: reply.ReduceInputFile,
			}
			reply1 := FinishTaskReply{}
			if err != nil {
				args1.Done = false
				fmt.Printf("reduce task failed [error]: %v\n", err)
			} else {
				args1.ReduceOutputFile = outputFile
				args1.Done = true
			}

			if !callByTCP("FinishTask", &args1, &reply1) && !reply1.Done {
				fmt.Printf("reduce task failed\n")
			}
			// }(args, reply)
		default:
			fmt.Printf("wrong task type\n")
			continue
		}
	}
}

//
// mapNumber: map任务编号; nReduce: reduce的任务数量; inputFile: 输入文件; mapf：更底层的map实现插件
// 将输入文件解析成Json格式的[key1:1, key2:1, key3:1, key2:1, key1:1, key3:1, ....], 并缓存到本地
func Map(mapNumber int, nReduce int, inputFile string, mapf func(string, string) []KeyValue) (outputFiles []string, err error) {
	inputfile, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}
	defer inputfile.Close()
	contents, err := ioutil.ReadAll(inputfile)
	if err != nil {
		return nil, err
	}
	kva := mapf(inputFile, string(contents))

	output := make([]*os.File, nReduce)
	outputEncoder := make([]*json.Encoder, nReduce)
	for index := 0; index < nReduce; index++ {
		output[index], err = ioutil.TempFile("", "mr-tmp-"+strconv.Itoa(mapNumber)+"-*")
		if err != nil {
			return nil, err
		}
		defer output[index].Close()
		outputEncoder[index] = json.NewEncoder(output[index])
	}

	for _, kv := range kva {
		err = outputEncoder[ihash(kv.Key)%nReduce].Encode(&kv)
		if err != nil {
			return nil, err
		}
	}

	for index, file := range output {
		newName := "mr-" + strconv.Itoa(mapNumber) + "-" + strconv.Itoa(index)
		err = os.Rename(filepath.Join(file.Name()), newName)
		outputFiles = append(outputFiles, newName)
		if err != nil {
			return nil, err
		}
	}
	return outputFiles, err
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// reduceNumber: reduce任务编号; inputFiles: 输入文件切片; reducef：更底层的reduce实现插件
// 将输入文件的json格式解析成[]KeyValue, 排序, 合并相同单词
func Reduce(reduceNumber int, inputFiles []string, reducef func(string, []string) string) (outputFile string, err error) {
	kva := make([]KeyValue, 0)
	for _, inputFileName := range inputFiles {
		file, err := os.Open(filepath.Join("", inputFileName))
		if err != nil {
			return "", err
		}
		defer file.Close()
		fileDecoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := fileDecoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	output, err := ioutil.TempFile("", "mr-tmp-out-"+strconv.Itoa(reduceNumber)+"*")
	if err != nil {
		return "", err
	}
	defer output.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value) // key:[1, 1, ....]
		}
		v := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(output, "%v %v\n", kva[i].Key, v)

		i = j
	}

	newName := "mr-out-" + strconv.Itoa(reduceNumber)
	err = os.Rename(filepath.Join(output.Name()), filepath.Join("", newName))
	if err != nil {
		return "", err
	}
	outputFile = newName
	return outputFile, err
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Coordinator.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

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

func callByTCP(rpcName string, args interface{}, reply interface{}) bool {
	client, err := rpc.Dial("tcp", "0.0.0.0:12345")
	if err != nil {
		log.Fatal("dialing:", err)
		// return false // coordinator宕机
	}
	err = client.Call(MRServiceName+"."+rpcName, args, reply)
	if err != nil {
		fmt.Printf("%v\n", err)
		return false
	}
	client.Close()
	return true
}
