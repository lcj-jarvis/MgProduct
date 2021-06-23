package fai.MgProductLibSvr.domain.common;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.lock.SimpleDistributedLock;
import fai.comm.distributedkit.lock.support.FaiLockGenerator;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:15
 */
public class LockUtil {

	private static final String LOCK_TYPE = "PRODUCTLIB_SVR_LOCK";
	private static final long M_RETRY_LOCK_TIME = 100L;
	private static int m_lockLease = 1000;
	private static FaiLockGenerator m_lockGenerator;

	public static void init(RedisCacheManager cache, int lockLease) {
		m_lockGenerator = new FaiLockGenerator(cache);
		m_lockLease = lockLease;
		LibLock.init(cache, lockLease);
		LibRelLock.init(cache, lockLease);
	}

	public static Lock getLock(int aid) {
		return m_lockGenerator.gen(LOCK_TYPE, String.valueOf(aid), m_lockLease, TimeUnit.MILLISECONDS, M_RETRY_LOCK_TIME);
	}

	public static class LibLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache, int lockLease) {
			m_readLock = new SimpleDistributedLock(cache, READ_LOCK_TYPE, lockLease, M_RETRY_LOCK_TIME);
		}
		private static SimpleDistributedLock m_readLock;

		private static final String READ_LOCK_TYPE = "LIB_READ_LOCK";
	}

	public static class LibRelLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache, int lockLease) {
			m_readLock = new SimpleDistributedLock(cache, READ_LOCK_TYPE, lockLease, M_RETRY_LOCK_TIME);
		}
		private static SimpleDistributedLock m_readLock;

		private static final String READ_LOCK_TYPE = "LIBREL_READ_LOCK";
	}
}
