package fai.MgProductSpecSvr.domain.comm;

import fai.comm.distributedkit.lock.support.FaiLockGenerator;
import fai.comm.util.Log;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class LockUtil {
    private LockUtil() {
    }

    public static void lock(int aid){
        LockWarp lockWarp = threadIdLockWarpCache.get(Thread.currentThread().getId());
        if(lockWarp == null){
            Lock lock = lockGenerator.gen(LOCK_TYPE, String.valueOf(aid), lockLease, TimeUnit.MILLISECONDS, retryLockTime);
            lockWarp = new LockWarp(aid, lock);
            LockWarp exists = threadIdLockWarpCache.put(Thread.currentThread().getId(), lockWarp);
            if(exists != null){
                Log.logErr("already exists;threadId=%s;aid=%s;exists=%s;", Thread.currentThread().getId(), aid, exists);
            }
        }
        if(lockWarp.holdingAid != aid){
            Log.logErr("holding err;threadId=%s;aid=%s;holdingAid=%s;", Thread.currentThread().getId(), aid, lockWarp.holdingAid);
        }
        lockWarp.count++;
        lockWarp.lock.lock();
    }

    public static void unlock(int aid){
        LockWarp lockWarp = threadIdLockWarpCache.get(Thread.currentThread().getId());
        if(lockWarp == null){
            throw new RuntimeException("unlock err aid="+aid);
        }
        lockWarp.lock.unlock();
        if(--lockWarp.count == 0){
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
        private int count;
        private Lock lock;
        public LockWarp(int holdingAid, Lock lock) {
            this.holdingAid = holdingAid;
            this.lock = lock;
            count = 0;
        }

        @Override
        public String toString() {
            return "LockWarp{" +
                    "holdingAid=" + holdingAid +
                    ", count=" + count +
                    ", lock=" + lock +
                    '}';
        }
    }


    public static final ConcurrentHashMap<Long, LockWarp> threadIdLockWarpCache = new ConcurrentHashMap<>();

    private static final String LOCK_TYPE = "MG_PRODUCT_SPEC_SVR_LOCK";
}
