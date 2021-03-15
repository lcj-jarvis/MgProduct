package fai.MgProductGroupSvr.domain.common;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.lock.PosDistributedLockPool;
import fai.comm.distributedkit.lock.support.FaiLockGenerator;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class LockUtil {

	public static void init(RedisCacheManager cache, int lockLease, int readLockLength) {
		m_lockGenerator = new FaiLockGenerator(cache);
		m_lockLease = lockLease;
		GroupLock.init(cache, lockLease, readLockLength);
		GroupRelLock.init(cache, lockLease, readLockLength);
	}

	public static Lock getLock(int aid) {
		return m_lockGenerator.gen(LOCK_TYPE, String.valueOf(aid), m_lockLease, TimeUnit.MILLISECONDS, m_retryLockTime);
	}

	public static class GroupLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache, int lockLease, int readLockLength) {
			m_readLock = new PosDistributedLockPool(cache, READ_LOCK_TYPE, readLockLength, readLockLength*2, lockLease, TimeUnit.MILLISECONDS, m_retryLockTime);
		}
		private static PosDistributedLockPool m_readLock;

		private static String READ_LOCK_TYPE = "PDGROUP_READ_LOCK";
	}

	public static class GroupRelLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache, int lockLease, int readLockLength) {
			m_readLock = new PosDistributedLockPool(cache, READ_LOCK_TYPE, readLockLength, readLockLength*2, lockLease, TimeUnit.MILLISECONDS, m_retryLockTime);
		}
		private static PosDistributedLockPool m_readLock;

		private static String READ_LOCK_TYPE = "PDGROUPREL_READ_LOCK";
	}

	private static String LOCK_TYPE = "MGPRODUCTGROUP_SVR_LOCK";
	private static long m_retryLockTime = 100L;
	private static int m_lockLease = 1000;
	private static FaiLockGenerator m_lockGenerator;
}
