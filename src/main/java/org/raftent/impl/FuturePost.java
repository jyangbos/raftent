package org.raftent.impl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class FuturePost implements Future<Boolean> {
	private Lock lock;
	private Condition cond;
	private boolean success;
	private boolean done;

	FuturePost() {
		this.lock = new ReentrantLock();
		this.cond = lock.newCondition();
		success = false;
		done = false;
	}

	public void setSuccess(boolean success) {
		this.success = success;
		this.done = true;
	}

	public Lock getLock() {
		return lock;
	}

	public Condition getCondition() {
		return cond;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public Boolean get() throws InterruptedException, ExecutionException {
		lock.lock();
		try {
			if (done) {
				return success;
			}
			cond.await();
		} finally {
			lock.unlock();
		}
		return success;
	}

	@Override
	public Boolean get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		lock.lock();
		try {
			if (done) {
				return success;
			}
			cond.await(timeout, unit);
		} finally {
			lock.unlock();
		}
		return success;
	}
}

