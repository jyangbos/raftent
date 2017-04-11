package org.raftent.node;

public interface StateMachine {
	String getName();
	void transitState(Object entry);
}
