package fai.MgProductSpecSvr.domain.repository;

import fai.comm.cache.redis.RedisCacheManager;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

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
	 * 清除缓存版本
	 * @param aid
	 * @return
	 */
	public static boolean clearCacheVersion(int aid){
		String cacheVersionKey = getCacheVersionKey(aid);
		return m_cache.del(cacheVersionKey);
	}

	protected static String wrapCacheVersion(String cacheKey, int aid){
		return cacheKey + ":"+getCacheVersion(aid);
	}
	/**
	 * 获取版本
	 * @param aid
	 * @return
	 */
	protected static String getCacheVersion(int aid){
		String cacheVersionKey = getCacheVersionKey(aid);
		String version = LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss"));
		Object res = m_cache.getRedisClient().eval(LUA_GET_OR_SET_EXPIRE, 1, cacheVersionKey, version, String.valueOf(m_cache.getExpireSecond()));
		version = res.toString();
		return version;
	}


	private static String getCacheVersionKey(int aid){
		return CACHE_VERSION_PREFIX + ":"+aid;
	}

	protected static final String CACHE_VERSION_PREFIX = "MG_productSpecSvrCacheVersion";

	/**
	 * 设置脏数据过期时间
	 */
	protected static final int DIRTY_EXPIRE_SECOND = 10;
	protected static RedisCacheManager m_cache;
	/**
	 * local val = get(key);
	 * if not val then
	 * 	set(key, arg1);
	 * 	val = arg1;
	 * end;
	 * expire(key, arg2);
	 * return val;
	 */
	private static final String LUA_GET_OR_SET_EXPIRE = "local val = redis.call('get', KEYS[1]); if not val then redis.call('set', KEYS[1], ARGV[1]); val = ARGV[1]; end; redis.call('expire', KEYS[1], ARGV[2]); return val;";
}
