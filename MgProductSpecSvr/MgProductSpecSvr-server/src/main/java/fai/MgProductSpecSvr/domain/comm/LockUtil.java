package fai.MgProductSpecSvr.domain.comm;

import fai.comm.distributedkit.lock.support.FaiLockGenerator;
import fai.comm.util.Log;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class LockUtil {
    private LockUtil() {
    }

    /**
     * 公共锁 加锁
     */
    public static void lock(int aid){
        commLock.lock(aid);
    }
    /**
     * 公共锁 解锁
     */
    public static void unlock(int aid){
        commLock.unlock(aid);
    }

    /**
     * 读锁 加锁
     */
    public static void readLock(int aid){
        readLock.lock(aid);
    }

    /**
     * 读锁 解锁
     */
    public static void unReadLock(int aid){
        readLock.unlock(aid);
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

    private static class AidLock {
        private ConcurrentHashMap<Long, LockWarp> threadIdLockWarpCache;
        private String lockType;

        public AidLock(String lockType) {
            this.threadIdLockWarpCache = new ConcurrentHashMap<>();
            this.lockType = lockType;
        }
        public void lock(int aid){
            LockWarp lockWarp = threadIdLockWarpCache.get(Thread.currentThread().getId());
            if(lockWarp == null){
                Lock lock = lockGenerator.gen(lockType, String.valueOf(aid), lockLease, TimeUnit.MILLISECONDS, retryLockTime);
                lockWarp = new LockWarp(aid, lock);
                LockWarp exists = threadIdLockWarpCache.put(Thread.currentThread().getId(), lockWarp);
                if(exists != null){
                    String msg = String.format("already exists;threadId=%s;aid=%s;exists=%s;", Thread.currentThread().getId(), aid, exists);
                    Log.logErr(msg);
                    throw new RuntimeException(msg);
                }
            }
            if(lockWarp.holdingAid != aid){
                String msg = String.format("holding err;threadId=%s;aid=%s;holdingAid=%s;", Thread.currentThread().getId(), aid, lockWarp.holdingAid);
                Log.logErr(msg);
                throw new RuntimeException(msg);
            }
            lockWarp.count++;
            lockWarp.lock.lock();
        }

        public void unlock(int aid){
            LockWarp lockWarp = threadIdLockWarpCache.get(Thread.currentThread().getId());
            if(lockWarp == null){
                throw new RuntimeException("unlock err aid="+aid);
            }
            lockWarp.lock.unlock();
            if(--lockWarp.count == 0){
                threadIdLockWarpCache.remove(Thread.currentThread().getId());
            }
        }
    }

    public static final AidLock commLock = new AidLock("MG_PRODUCT_SPEC_SVR_LOCK");
    public static final AidLock readLock = new AidLock("MG_PRODUCT_SPEC_SVR_READ_LOCK");
}
