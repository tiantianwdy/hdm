package org.hdm.core.utils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by tiantian on 16/05/16.
 */
public class NotifyLock {

    private  Integer lock = 0;

    private AtomicBoolean valid = new AtomicBoolean(false);


    public void acquire() throws InterruptedException{
        synchronized(lock) {
            valid.set(false);
            lock.wait();
        }
    }

    public void release(){
        synchronized(lock)  {
            lock.notifyAll();
            valid.set(true);
        }
    }

    public Boolean isAvailable(){
        return valid.get();
    }


}
