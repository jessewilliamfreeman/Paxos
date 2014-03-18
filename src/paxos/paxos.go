package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"



type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  agreements map[int]interface{}
  maxPrepare int
  maxAccept int
  maxAcceptValue *Proposal
  doneValue int
  max int
  // Your data here.
}

type Proposal struct {
  Seq int
  Value interface{}
}

type PrepareArgs struct {
  N int
}

type PrepareReply struct {
  N int
  Value *Proposal
  Success bool
}

type AcceptArgs struct {
  N int
  Value *Proposal
}

type AcceptReply struct {
  N int
  Success bool
}

type DecideArgs struct {
  Value *Proposal
}

type DecideReply struct {
  Success bool
}

type MinArgs struct {
}

type MinReply struct {
  N int
  Success bool
}



//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

// rpc handlers
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
//  for key := range px.agreements {
//    if key == args.N {
//	  reply.N = key
//	  reply.Value = px.agreements[key]
//	  reply.Success = true
//	  return nil
//	}
//  }
  if args.N <= px.maxPrepare {
    reply.Success = false
	reply.N = px.maxPrepare
	return nil
  } else {
    px.maxPrepare = args.N
	reply.N = px.maxAccept
	reply.Value = px.maxAcceptValue
	reply.Success = true
	return nil
  } 
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
//  for key := range px.agreements {
//    if key == args.N {
//	  reply.N = px.maxAccept
//	  reply.Success = true
//	}
//  }
  if args.N < px.maxPrepare {
    reply.Success = false
	return nil
  } else {
    px.maxPrepare = args.N
	px.maxAccept = args.N
	px.maxAcceptValue = args.Value
	reply.N = px.maxAccept
	reply.Success = true
	return nil
  } 
} 

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  px.agreements[args.Value.Seq] = args.Value.Value
  reply.Success = true
  go func() {
    decided := []string
    for key, server := range px.peers {
	  decided[key] = server
	}
	for args.Value.Seq > px.min && !(px.dead) {
	  for 
	}
  }()
  return nil
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  notDecided := true
  // for key, _ := range px.agreements {
  //  if key == seq {
  //  notDecided = false
  //	}
  //}
  if (seq > px.doneValue && notDecided && !(px.dead)) {
    go func() {
      // fmt.Println(seq)
	  // fmt.Println(v)
	  notDecided := true
	  proposerN := px.maxPrepare + 1
	  proposerValue := &Proposal{seq, v}
	  for notDecided && !(px.dead) {
        props_acpted := 0
	    values := map[int]PrepareReply{}
        for key, server := range px.peers {
  	      args := &PrepareArgs{}
	      args.N = proposerN
          var reply PrepareReply
	      reply.Success = false
	      if key == px.me {
	        px.Prepare(args, &reply)
	      } else {
	        call(server, "Paxos.Prepare", args, &reply)
	      }
	      if reply.Success {
	        values[key] = reply
	        props_acpted += 1
	      }
        }
		// fmt.Println(len(px.peers))
	    if props_acpted >= (len(px.peers) / 2) + 1 {
	      // fmt.Println(seq)
	      // fmt.Println(v)
	      acceptN := proposerN
	      acceptValue := proposerValue
	      for _, value := range values {
	        if value.N >= acceptN {
		      acceptN = value.N
		      acceptValue = value.Value
		    }
	      }
	      acpts := 0
	      for key , server := range px.peers {
	        args := &AcceptArgs{}
	        args.N = acceptN
	        args.Value = acceptValue
            var reply AcceptReply
		    reply.Success = false
		    if key == px.me {
		      px.Accept(args, &reply)
		    } else {
	          call(server, "Paxos.Accept", args, &reply)
		    }
		    if reply.Success {
		      acpts += 1
		    }
          }
	      if acpts >= (len(px.peers) / 2) + 1{
	        for key , server := range px.peers {
		      args := &DecideArgs{}
	          args.Value = acceptValue
              var reply DecideReply
		      reply.Success = false
		      if key == px.me {
		        px.Decide(args, &reply)
		      } else {
	            call(server, "Paxos.Decide", args, &reply)
		      }
		    }
			notDecided = false
          } else {
		    if proposerN < px.maxAccept {
			  proposerN = px.maxAccept + 1
			} else {
		      proposerN++
			}
		  }
		} else {
		  if proposerN < px.maxAccept {
			proposerN = px.maxAccept + 1
	      } else {
		    proposerN++
		  }
		}
	  }
    }()
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  dn := seq
//  mn := px.Min()
//  if seq > mn {
//    dn = mn
//  }
  for key, _ := range px.agreements {
    if key <= dn {
	  delete(px.agreements, key)
	}
  }
  px.doneValue = dn
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  max := -1
  for key, _  := range px.agreements {
    if key > max {
	  max = key
	}
  }
  return max
}

func (px *Paxos) RequestMin(args *MinArgs, reply *MinReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  reply.N = px.doneValue
  reply.Success = true
  return nil
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  ns := map[int]int{}
  for key, server := range px.peers {
	if key != px.me {
	  args := &MinArgs{}
      var reply MinReply
	  reply.Success = false
	  call(server, "Paxos.RequestMin", args, &reply)
	  if reply.Success {
        ns[key] = reply.N
	  } else {
	    ns[key] = -1
	  }
	}
  }
  min := px.doneValue
  for _, n := range ns {
    if min > n {
	  min = n
	}
  }
  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  for key, value := range px.agreements {
    if key == seq {
	  return true, value
	}
  }
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.agreements = map[int]interface{}{}
  px.maxPrepare = -1
  px.maxAccept = -1
  px.doneValue = -1
  px.max = -1
  
  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
