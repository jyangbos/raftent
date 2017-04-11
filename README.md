# raftent
A Java implementation of Raft in support of multiple state machines

Features

1. A small footprint Raft library (not a standalone service) that can be included in any service so that such service is able to maintain its own distributed state machines. The service that uses Raftent library is a host service.

2. Raftent supports multiple independent state machines. A host service could be the leader for zero or more independent state machines.

Build

The build tool is gradle.

Usage Example

Step 1. Create a class implementing the StateMachine interface.
<pre>
public class Fsm implements StateMachine {
  private String name;

  public Fsm(String name) {
    this.name = name;
  }

  @Override
  public void transitState(Object entry) {
    // noop
  }

  @Override
  public String getName() {
    return name;
  }		
}
</pre>

Step 2. Create a Raft node.
<pre>
  String[] nodes = new String[] {"127.0.0.1", "127.0.0.1", "127.0.0.1"};
  int nodeId = 0;
  raftNode = RaftNodeFactory.newInstance(nodes, nodeId, "logdata").newRaftNode();
  raftNode.start();
</pre>
<code>nodes</code> is a list of IP addresses of service instances. <code>nodeId</code> is an index to the list, which identifies the node. "logdata" is the prefix of persistent files.

Step 3. Get a <code>LogProposal</code> object.
<pre>
  LogProposal logProposer = raftNode.getLogProposal(new Fsm("fsm0");
</pre>

Step 4. Propose log entry.
<pre>
  Future<Boolean> result = logProposer.post(new Data("mydata0"))
	if (result != null) {
		System.out.println(String.format("post data %b", result.get()));
	}
	else {
		System.out.println("result is null");
	}
</pre>
When the services start up, Raft needs to elect a leader. Initial <code>post</code> may return <code>null</code> if a leader has not been elected.
