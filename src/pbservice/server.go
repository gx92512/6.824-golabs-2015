package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
    cview      viewservice.View
    data       map[string]Value
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
    v,ok := pb.data[args.Key]
    if ok{
        reply.Value = v.Data
        reply.Err = OK
    }else{
        reply.Err = ErrNoKey
    }

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
    if args.Direct{
        if pb.cview.Primary != pb.me{
            reply.Err = ErrWrongServer
            return nil
        }else{
            if pb.cview.Backup != ""{
                args2 := args
                args2.Direct = false
                var reply2 PutAppendReply
                call(pb.cview.Backup, "PBServer.PutAppend", args2, &reply2)
                fmt.Printf("call backup %s  %s  %s\n", pb.cview.Backup, args.Key, args.Value)
                if reply2.Err == ErrWrongServer{
                    reply.Err = ErrWrongServer
                    return nil
                }
            }
        }
    }else{
        if pb.cview.Backup != pb.me{
            reply.Err = ErrWrongServer
            return nil
        }
    }
    if pb.data[args.Key].Id == args.Id{
        return nil
    }
    if args.Op == "Put"{
        d := Value{args.Value, args.Id}
        pb.data[args.Key] = d
        //pb.data[args.Key].Data = args.Value
        //pb.data[args.Key].Id = args.Id
    }else{
        v := pb.data[args.Key].Data
        d := Value{v + args.Value, args.Id}
        pb.data[args.Key] = d
        //pb.data[args.Key].Data = v + args.Value
        //pb.data[args.Key].Id = args.Id
    }
    fmt.Printf("sever put %s  %s  %s\n", pb.me, args.Key, pb.data[args.Key].Data)
    //fmt.Println(pb.data)
    reply.Err = OK

	return nil
}

func (pb *PBServer) Trans(args map[string]Value, reply *TransReply) error{
    fmt.Println(args)
    fmt.Printf("Trans %s %s \n", pb.cview.Backup, pb.me)
    /*
    if pb.cview.Backup == pb.me{
        pb.data = args
        reply.Err = OK
    }else{
        reply.Err = ErrWrongServer
    }
    */
    pb.data = args
    reply.Err = OK
    return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
    for pb.isdead() == false{
        nview, _ := pb.vs.Ping(pb.cview.Viewnum)
        if nview.Viewnum != pb.cview.Viewnum{
            fmt.Printf("server tick %s %s %s\n", pb.cview.Primary, nview.Backup, pb.cview.Backup)
            if pb.cview.Primary == pb.me && pb.cview.Backup != nview.Backup{
                /*
                for k,v := range pb.data{
                    args := &PutAppendArgs{}
                    args.Key = k
                    args.Value = v
                    args.Op = "Put"
                    args.Direct = false
                    var reply PutAppendReply
                    call(nview.Backup, "PBServer.PutAppend", args, &reply)
                    fmt.Printf("server tick %s %s\n", nview.Backup, reply.Err)
                }
                */
                //args := &TransArgs{data: map[string]string{}}
                //args.data = pb.data
                var reply TransReply
                call(nview.Backup, "PBServer.Trans", pb.data, &reply)
            }
            pb.cview = nview
        }
        time.Sleep(viewservice.PingInterval)
    }
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
    pb.cview.Viewnum = 0
    pb.data = make(map[string]Value)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
