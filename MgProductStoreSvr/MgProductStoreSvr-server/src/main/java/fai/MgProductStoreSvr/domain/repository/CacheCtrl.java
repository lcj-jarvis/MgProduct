package fai.MgProductStoreSvr.domain.repository;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.client.RedisClient;

public class CacheCtrl {

	public static boolean set(String key, String value){
		RedisClient redisClient = m_cache.getRedisClient();
		String res = redisClient.set(key, value);
		return RedisCacheManager.REDIS_RSP_OK.equals(res);
	}

	public static Long incby(String key, long value){
		RedisClient redisClient = m_cache.getRedisClient();
		Long result = redisClient.incrBy(key, value);
		return result;
	}



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
