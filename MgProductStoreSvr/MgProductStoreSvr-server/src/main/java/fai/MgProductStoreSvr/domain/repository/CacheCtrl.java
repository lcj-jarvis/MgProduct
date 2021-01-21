package fai.MgProductStoreSvr.domain.repository;

import fai.comm.cache.redis.RedisCacheManager;

public class CacheCtrl {
	/**
	 * svr启动时调用
	 * @param cache
	 */
	public static boolean init(RedisCacheManager cache) {
		if(m_cache != null) {
			return true;
		}
		if(cache == null) {
			return false;
		}
		m_cache = cache;
		return true;
	}

	/**
	 * 设置脏数据过期时间
	 */
	protected static final int DIRTY_EXPIRE_SECOND = 10;
	protected static RedisCacheManager m_cache;
}
