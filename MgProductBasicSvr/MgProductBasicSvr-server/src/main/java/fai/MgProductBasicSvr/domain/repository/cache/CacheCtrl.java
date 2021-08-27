package fai.MgProductBasicSvr.domain.repository.cache;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.client.RedisClientExecutor;
import fai.comm.cache.redis.pool.JedisPool;
import fai.comm.util.*;
import fai.middleground.infutil.MgConfPool;
import redis.clients.jedis.Jedis;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.List;

/**
 * 方便统一初始化各个CacheCtrl的RedisCacheManager
 */
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

	private static String getCacheVersionKey(int aid){
		return CACHE_VERSION_PREFIX + ":"+aid;
	}

	protected static String wrapCacheVersion(String cacheKey, int aid){
		return cacheKey + cacheSuffix + ":" + getCacheVersion(aid);
	}

	protected static final String CACHE_VERSION_PREFIX = "MG_pdBasicCacheVer";

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


	protected static Object eval(byte[] script, int keyCount, byte[]... params) {

		return new RedisClientExecutor<Object>(m_jedisPool) {
			@Override
			public Object  execute(Jedis jedis) {
				return jedis.eval(script, keyCount, params);
			}
		}.run();
	}

	public static FaiList<Param> getFaiList(FaiList<String> cacheKeys, ParamDef infoDtoDef, int infoKey) {
		byte[][] keyBytes = new byte[cacheKeys.size()][];
		for(int i = 0; i < cacheKeys.size(); i++) {
			keyBytes[i] = cacheKeys.get(i).getBytes();
		}

		FaiList<Param> list = new FaiList<>();
		List<byte[]> res = (List<byte[]>)eval(GET_KEYS_SCRIPT.getBytes(), keyBytes.length, keyBytes);
		for(int i = 0; i < res.size(); i++) {
			byte[] bytes = res.get(i);
			if(bytes ==null) {
				continue;
			}
			FaiList<Param> curList = new FaiList<>();
			ByteBuffer buf = ByteBuffer.wrap(bytes);
			Ref<Integer> keyRef = new Ref<Integer>();
			int rt = curList.fromBuffer(buf, keyRef, infoDtoDef);
			if (rt != Errno.OK || keyRef.value != infoKey) {
				curList = null;
				Log.logErr("list from buffer err");
			}
			list.addAll(curList);
		}
		return list;
	}

	private static final String GET_KEYS_SCRIPT = "local result = {};for i = 1,#(KEYS) do result[i]= redis.call('get',KEYS[i]) end; return result";

	public static void init(RedisCacheManager cache, JedisPool jedisPool, String suffix) {
		m_cache = cache;
		m_jedisPool = jedisPool;
		if(!Str.isEmpty(suffix)) {
			cacheSuffix = "_" + suffix;
		}
	}

	protected static String cacheSuffix = "";
	protected static RedisCacheManager m_cache;
	protected static JedisPool m_jedisPool;
}
