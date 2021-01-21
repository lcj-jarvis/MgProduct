package fai.MgProductStoreSvr.domain.comm;

import fai.comm.distributedkit.lock.support.FaiLockGenerator;
import fai.comm.util.Log;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public class LockUtil {

    private LockUtil() {
    }

    public static void lock(int aid){
        LockWarp lockWarp = threadIdLockWarpCache.get(Thread.currentThread().getId());
        if(lockWarp == null){
            Lock lock = lockGenerator.gen(LOCK_TYPE, String.valueOf(aid), lockLease, TimeUnit.MILLISECONDS, retryLockTime);
            lockWarp = new LockWarp(aid, lock);
            threadIdLockWarpCache.putIfAbsent(Thread.currentThread().getId(), lockWarp);
            Log.logDbg("whalelog  threadId=%s;aid=%s", Thread.currentThread().getId(), aid);
        }
        if(lockWarp.holdingAid != aid){
            Log.logErr("holding err;threadId=%s;aid=%s;holdingAid=%s;", Thread.currentThread().getId(), aid, lockWarp.holdingAid);
        }
        lockWarp.count.incrementAndGet();
        lockWarp.lock.lock();
    }

    public static void unlock(int aid){
        LockWarp lockWarp = threadIdLockWarpCache.get(Thread.currentThread().getId());
        if(lockWarp == null){
            throw new RuntimeException("unlock err aid="+aid);
        }
        lockWarp.lock.unlock();
        if(lockWarp.count.decrementAndGet() == 0){
            Log.logDbg("whalelog  threadId=%s;aid=%s", Thread.currentThread().getId(), aid);
            threadIdLockWarpCache.remove(Thread.currentThread().getId());
        }
    }

    public static void initLock(FaiLockGenerator lockGenerator, int lockLease, long retryLockTime){
        LockUtil.lockGenerator = lockGenerator;
        LockUtil.lockLease = lockLease;
        LockUtil.retryLockTime = retryLockTime;

    }
    private static FaiLockGenerator lockGenerator;
    private static int lockLease;
    private static long retryLockTime;
    private static class LockWarp{
        private int holdingAid;
        private AtomicInteger count;
        private Lock lock;
        public LockWarp(int holdingAid, Lock lock) {
            this.holdingAid = holdingAid;
            this.lock = lock;
            count = new AtomicInteger();
        }
    }


    public static final ConcurrentHashMap<Long, LockWarp> threadIdLockWarpCache = new ConcurrentHashMap<>();

    private static final String LOCK_TYPE = "MG_PRODUCT_STORE_SVR_LOCK";
}
