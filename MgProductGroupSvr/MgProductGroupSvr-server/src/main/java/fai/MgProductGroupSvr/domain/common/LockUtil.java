package fai.MgProductGroupSvr.domain.common;

import fai.MgProductGroupSvr.application.MgProductGroupSvr;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.lock.PosDistributedLockPool;
import fai.comm.distributedkit.lock.SimpleDistributedLock;
import fai.comm.distributedkit.lock.support.FaiLockGenerator;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class LockUtil {

	public static void init(RedisCacheManager cache, MgProductGroupSvr.LockOption lockOption) {
		m_lockOption = lockOption;
		GroupLock.init(cache);
		GroupRelLock.init(cache);
		BackupLock.init(cache);
		m_lock = new SimpleDistributedLock(cache, LOCK_TYPE, lockOption.getLockLease(), m_retryLockTime, lockOption.getLockLength());
	}

	public static void lock(int aid) {
		m_lock.lock(aid);
	}

	public static void unlock(int aid) {
		m_lock.unlock(aid);
	}

	public static class GroupLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache) {
			m_readLock = new SimpleDistributedLock(cache, READ_LOCK_TYPE, m_lockOption.getLockLease(), m_retryLockTime, m_lockOption.getReadLockLength());
		}
		private static SimpleDistributedLock m_readLock;

		private static String READ_LOCK_TYPE = "pdGroupRead";
	}

	public static class GroupRelLock {
		public static void readLock(int aid) {
			m_readLock.lock(aid);
		}

		public static void readUnLock(int aid) {
			m_readLock.unlock(aid);
		}

		public static void init(RedisCacheManager cache) {
			m_readLock = new SimpleDistributedLock(cache, READ_LOCK_TYPE, m_lockOption.getLockLease(), m_retryLockTime, m_lockOption.getReadLockLength());
		}
		private static SimpleDistributedLock m_readLock;

		private static String READ_LOCK_TYPE = "pdGroupRelRead";
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
		private static String LOCK_TYPE = "mgPdGroupBak";
	}

	private static String LOCK_TYPE = "mgPdGroupSvr";
	private static long m_retryLockTime = 100L;
	private static MgProductGroupSvr.LockOption m_lockOption;
	private static SimpleDistributedLock m_lock;
}
