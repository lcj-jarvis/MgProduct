package fai.MgProductPropSvr.domain.repository;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.util.Parser;

import java.util.Calendar;

public class CacheCtrl {
	/**
	 * 获取版本
	 */
	protected static String getCacheVersion(int aid){
		String cacheVersionKey = getCacheVersionKey(aid);
		String version = Parser.parseString(Calendar.getInstance(), "HHmmss");
		Object res = m_cache.getRedisClient().eval(LUA_GET_OR_SET_EXPIRE, 1, cacheVersionKey, version, String.valueOf(m_cache.getExpireSecond()));
		version = res.toString();
		return version;
	}

	/**
	 * 清除缓存版本
	 */
	public static boolean clearCacheVersion(int aid){
		String cacheVersionKey = getCacheVersionKey(aid);
		return m_cache.del(cacheVersionKey);
	}

	public static boolean clearAllCache(int aid) {
		// 更新缓存数据版本号
		boolean success = CacheCtrl.clearCacheVersion(aid);

		// 尽可能删除已失效的缓存数据
		ProductPropCacheCtrl.delCache(aid);
		ProductPropValCacheCtrl.DataStatusCache.delCache(aid);

		return success;
	}

	private static String getCacheVersionKey(int aid){
		return CACHE_VERSION_PREFIX + ":"+aid;
	}

	protected static String wrapCacheVersion(String cacheKey, int aid){
		return cacheKey + ":"+getCacheVersion(aid);
	}

	protected static final String CACHE_VERSION_PREFIX = "MG_pdPropCacheVer";

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

	public static void init(RedisCacheManager cache) {
		m_cache = cache;
	}

	protected static RedisCacheManager m_cache;
}
