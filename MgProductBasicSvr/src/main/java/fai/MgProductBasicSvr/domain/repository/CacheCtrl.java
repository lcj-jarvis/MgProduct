package fai.MgProductBasicSvr.domain.repository;

import fai.comm.cache.redis.RedisCacheManager;

public class CacheCtrl {
	public static void init(RedisCacheManager cache) {
		m_cache = cache;
	}

	protected static RedisCacheManager m_cache;
}
