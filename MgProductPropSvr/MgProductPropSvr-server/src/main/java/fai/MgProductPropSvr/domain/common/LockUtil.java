package fai.MgProductPropSvr.domain.common;

import fai.MgProductPropSvr.application.MgProductPropSvr;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.lock.SimpleDistributedLock;
import fai.comm.distributedkit.lock.support.FaiLockGenerator;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class LockUtil {

	public static void init(RedisCacheManager cache, MgProductPropSvr.LockOption lockOption) {
		m_lockGenerator = new FaiLockGenerator(cache);
		m_lockOption = lockOption;
		PropLock.init(cache);
		PropRelLock.init(cache);
		PropValLock.init(cache);
		BackupLock.init(cache);
	}

	public static Lock getLock(int aid) {
		return m_lockGenerator.gen(LOCK_TYPE, String.valueOf(aid), m_lockOption.getLockLease(), TimeUnit.MILLISECONDS, m_retryLockTime);
	}

	public static class PropLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache) {
			m_readLock = new SimpleDistributedLock(cache, READ_LOCK_TYPE, m_lockOption.getLockLease(), m_retryLockTime);
		}
		private static SimpleDistributedLock m_readLock;

		private static String READ_LOCK_TYPE = "PDPROP_READ_LOCK";
	}

	public static class PropRelLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache) {
			m_readLock = new SimpleDistributedLock(cache, READ_LOCK_TYPE, m_lockOption.getLockLease(), m_retryLockTime);
		}
		private static SimpleDistributedLock m_readLock;

		private static String READ_LOCK_TYPE = "PDPROPREL_READ_LOCK";
	}

	public static class PropValLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache) {
			m_readLock = new SimpleDistributedLock(cache, READ_LOCK_TYPE, m_lockOption.getLockLease(), m_retryLockTime);
		}
		private static SimpleDistributedLock m_readLock;

		private static String READ_LOCK_TYPE = "PDPROPVAL_READ_LOCK";
	}

	/**
	 * 备份锁
	 */
	public static class BackupLock {
		public static void lock(int aid) {
			bakLock.lock(aid);
		}

		public static void unlock(int aid) {
			bakLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache) {
			bakLock = new SimpleDistributedLock(cache, LOCK_TYPE, m_lockOption.getLockLease(), m_retryLockTime, m_lockOption.getBakLockLength());
		}

		private static SimpleDistributedLock bakLock;
		private static String LOCK_TYPE = "mgPdPropBak";
	}

	private static String LOCK_TYPE = "PRODUCTPROP_SVR_LOCK";
	private static long m_retryLockTime = 100L;
	private static MgProductPropSvr.LockOption m_lockOption;
	private static FaiLockGenerator m_lockGenerator;
}
