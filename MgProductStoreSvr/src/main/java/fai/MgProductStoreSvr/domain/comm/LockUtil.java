package fai.MgProductStoreSvr.domain.comm;

import fai.comm.distributedkit.lock.PosDistributedLock;

public class LockUtil {
    public static void lock(int aid){
        lock.lock(aid);
    }

    public static void unlock(int aid){
        lock.unlock(aid);
    }


    private static PosDistributedLock lock;
    public static void initLock(PosDistributedLock lock){
        LockUtil.lock = lock;
    }
}
