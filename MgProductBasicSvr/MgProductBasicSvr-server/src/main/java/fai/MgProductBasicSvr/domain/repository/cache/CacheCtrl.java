package fai.MgProductBasicSvr.domain.repository.cache;

import fai.comm.cache.redis.RedisCacheManager;

/**
 * 方便统一初始化各个CacheCtrl的RedisCacheManager
 */
public class CacheCtrl {
	public static void init(RedisCacheManager cache) {
		m_cache = cache;
	}

	protected static RedisCacheManager m_cache;
}
