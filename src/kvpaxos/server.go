package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  Name string
  IsPut bool
  Key string
  Value string
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  // Your definitions here.
  valueMap map[string]Op
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
    for i := kv.px.Min(); i <= kv.px.Max(); i++ {
      ok, log := kv.px.Status(i)
	  if ok {
	    op := log.(Op)
		if op.IsPut {
	      kv.valueMap[op.Key] = op
		}
	  }
    }
//	for key, value := range kv.valueMap {
//	  fmt.Println(key)
//	  fmt.Println(value)
//	  fmt.Println(value.Value)
//	}
    keyValue := Op{}
	keyValue.Name = args.Name
    keyValue.Key = args.Key
    keyValue.IsPut = false
	seq, notDuplicate := kv.callPaxos(keyValue)
	if notDuplicate {
	  ok, paxVal := kv.px.Status(seq)
	  if ok {
	    if paxVal == keyValue {
	      op, ok := kv.valueMap[args.Key]
	      if ok {
	        reply.Err = OK
	        reply.Value = op.Value
//		    fmt.Println(reply.Value)
	      } else {
	        reply.Err = ErrNoKey
	      }
	      kv.px.Done(seq)
//	  fmt.Println(reply)
	    }
	  }
	}

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
//    fmt.Println(args.Key)
    for i := 0; i <= kv.px.Max(); i++ {
      ok, log := kv.px.Status(i)
	  if ok {
	    op := log.(Op)
		if op.IsPut {
	      kv.valueMap[op.Key] = op
		}
	  }
    }
	fmt.Println(args.Key)
    keyValue := Op{}
	keyValue.Name = args.Name
    keyValue.Key = args.Key
    keyValue.Value = args.Value
    keyValue.IsPut = true
	seq, notDuplicate := kv.callPaxos(keyValue)
//	fmt.Println(keyValue.Value)
//	fmt.Println(notDuplicate)
	if notDuplicate {
//	  fmt.Println(keyValue.Value)
      ok, paxVal := kv.px.Status(seq)
	  if ok {
	    if paxVal == keyValue {
	      if op, alsook := kv.valueMap[args.Key]; alsook {
	        reply.PreviousValue = op.Value
	      }
	      kv.valueMap[args.Key] = keyValue
	      kv.px.Done(seq)
		}
	  }
	}

  return nil
}

func (kv *KVPaxos) callPaxos (keyValue Op) (int, bool) {
  seq := kv.px.Max()
  notComplete := true 
  for notComplete {
    notComplete = false
    seq++
	if seq <= kv.px.Max() {
	  seq = kv.px.Max() + 1
	}
	
    kv.px.Start(seq, keyValue);
	
	to := 10 * time.Millisecond
    decided := false
    for !(decided) && to < 100 * time.Millisecond{
	  var value interface{}
      decided, value = kv.px.Status(seq)
//	  fmt.Println(decided)
	  if decided && value != keyValue {
	    notComplete = true
	  } else {
	    time.Sleep(to)
	    if to < 10 * time.Second {
		  to *= 2
	    }
	  }
    }
  }
  return seq, true
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // this call is all that's needed to persuade
  // Go's RPC library to marshall/unmarshall
  // struct Op.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me
  
  kv.valueMap = map[string]Op{}

  // Your initialization code here.

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

