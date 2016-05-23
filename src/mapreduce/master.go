package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) killWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) runMaster() *list.List {
	// Your code here
    go func(){
        for mr.alive{
            if value, ok := <-mr.registerChannel; ok{
                args := &WorkerInfo{}
                args.address = value
                mr.Workers[value] = args
                mr.freeChannel <- value
            }else{
                break
            }
        }
    }()
    fmt.Printf("in runMaser %d\n", nMap)
    for i := 0; i < nMap; i++ {
        //fmt.Printf("in runMaser loop %d\n", i)
        t := i
        go func(){
            args := &DoJobArgs{}
            var reply DoJobReply
            args.File = mr.file
            args.Operation = Map
            args.JobNumber = t
            args.NumOtherPhase = mr.nReduce
            //fmt.Printf("in runMaser loop %d\n", t)
            for {
                if value, yes := <-mr.freeChannel; yes{
                //fmt.Printf("in runMaser rpc call %s\n", value)
                ok := call(value, "Worker.DoJob", args, &reply)
                if ok == false {
                    fmt.Printf("MapJob: RPC %s doJob error\n", value)
                }else{
                mr.freeChannel <- value
                mr.doneChannel <- 1
                break
            }
            }
        }
        }()
    }
    for i := 0; i < nMap; i++ {
        <-mr.doneChannel
    }
    for i := 0; i < mr.nReduce; i++ {
        t := i
        go func(){
            args := &DoJobArgs{}
            var reply DoJobReply
            args.File = mr.file
            args.Operation = Reduce
            args.JobNumber = t
            args.NumOtherPhase = mr.nMap
            for {
                if value, yes := <-mr.freeChannel; yes{
                ok := call(value, "Worker.DoJob", args, &reply)
                if ok == false {
                    fmt.Printf("ReduceJob: RPC %s doJob error\n", value)
                }else{
                    mr.freeChannel <- value
                    mr.doneChannel <- 1
                    break
                }
            }
        }
        }()
    }
    for i := 0; i < nReduce; i++ {
        <-mr.doneChannel
    }
	return mr.killWorkers()
}
