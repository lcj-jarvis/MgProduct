package fai.MgProductSpecSvr.domain.repository;

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


	protected static RedisCacheManager m_cache;
}
