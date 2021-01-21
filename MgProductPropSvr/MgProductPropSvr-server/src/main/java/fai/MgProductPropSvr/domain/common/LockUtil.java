package fai.MgProductPropSvr.domain.common;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.lock.support.FaiLockGenerator;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class LockUtil {

	public static void init(RedisCacheManager cache, int lockLease) {
		m_lockGenerator = new FaiLockGenerator(cache);
		m_lockLease = lockLease;
	}

	public static Lock getLock(int aid) {
		return m_lockGenerator.gen(LOCK_TYPE, String.valueOf(aid), m_lockLease, TimeUnit.MILLISECONDS, m_retryLockTime);
	}

	private static String LOCK_TYPE = "PRODUCTPROP_SVR_LOCK";
	private static long m_retryLockTime = 100L;
	private static int m_lockLease = 1000;
	private static FaiLockGenerator m_lockGenerator;
}
