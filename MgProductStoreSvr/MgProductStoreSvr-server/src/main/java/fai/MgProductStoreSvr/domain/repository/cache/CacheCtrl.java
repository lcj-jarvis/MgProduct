package fai.MgProductStoreSvr.domain.repository.cache;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.client.RedisClient;
import fai.comm.util.Log;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class CacheCtrl {

	public static boolean set(String key, String value){
		RedisClient redisClient = m_cache.getRedisClient();
		String res = redisClient.set(key, value);
		return RedisCacheManager.REDIS_RSP_OK.equals(res);
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
	 * 清除缓存版本
	 * @param aid
	 * @return
	 */
	public static boolean clearCacheVersion(int aid){
		String cacheVersionKey = getCacheVersionKey(aid);
		return m_cache.del(cacheVersionKey);
	}

	public static boolean clearAllCache(int aid) {
		boolean allOption = true;
		boolean boo = CacheCtrl.clearCacheVersion(aid);
		if(!boo){
			Log.logErr("CacheCtrl.clearCacheVersion err;aid=%s;", aid);
		}
		allOption &= boo;
		boo = SpuSummaryCacheCtrl.delAllCache(aid);
		if(!boo){
			Log.logErr("SpuSummaryCacheCtrl.delAllCache err;aid=%s;", aid);
		}
		allOption &= boo;

		return allOption;
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

	public static RedisCacheManager getCache() {
		return m_cache;
	}


	private static String getCacheVersionKey(int aid){
		return CACHE_VERSION_PREFIX + ":"+aid;
	}

	protected static final String CACHE_VERSION_PREFIX = "MG_productStoreSvrCacheVersion";


	/**
	 * 设置脏数据过期时间
	 */
	protected static final int DIRTY_EXPIRE_SECOND = 2;
	/**
	 * 缓存过期随机数，避免同一时间大量key失效
	 */
	protected static final int DIRTY_EXPIRE_SECOND_RANDOM = 4;

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
