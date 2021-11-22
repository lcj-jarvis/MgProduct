package fai.MgProductStoreSvr.domain.repository.cache;

import fai.MgProductStoreSvr.domain.comm.SkuBizKey;
import fai.MgProductStoreSvr.domain.repository.cache.CacheCtrl;
import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.comm.cache.redis.client.RedisClient;
import fai.comm.util.Errno;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.Parser;

import java.util.Arrays;
import java.util.Set;

public class StoreSalesSkuCacheCtrl extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, int pdId) {
        String cacheKey = getCacheKey(aid, unionPriId, pdId);
        return m_cache.getFaiList(cacheKey, StoreSalesSkuDto.Key.INFO, StoreSalesSkuDto.getInfoDto());
    }

    public static void delCache(int aid, int unionPriId, int pdId) {
        String cacheKey = getCacheKey(aid, unionPriId, pdId);
        if(m_cache.exists(cacheKey)) {
            m_cache.del(cacheKey);
        }
    }
    public static void setCacheList(int aid, int unionPriId, int pdId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId, pdId);
        m_cache.setFaiList(cacheKey, list, StoreSalesSkuDto.Key.INFO, StoreSalesSkuDto.getInfoDto());
    }

    public static boolean existsRemainCount(int aid, int unionPriId, long skuId){
        String remainCountCacheKey = getRemainCountCacheKey(aid, unionPriId, skuId);
        return m_cache.exists(remainCountCacheKey);
    }

    public static int initRemainCount(int aid, int unionPriId, long skuId, int count){
        String remainCountCacheKey = getRemainCountCacheKey(aid, unionPriId, skuId);
        if(m_cache.set(remainCountCacheKey, String.valueOf(count))){
            return Errno.OK;
        }
        return Errno.ERROR;
    }

    /**
     * 扣减剩余库存
     * @param count 数量 值大于0
     * @param zeroCountExpireTime 扣减后数量为0时，缓存的过期时间 单位s
     */
    public static int reduceRemainCount(int aid, int unionPriId, long skuId, int count, int zeroCountExpireTime){
        String remainCountCacheKey = getRemainCountCacheKey(aid, unionPriId, skuId);
        RedisClient redisClient = m_cache.getRedisClient();
        Object result = redisClient.eval(LuaScript.REDUCE_REMAIN_COUNT, Arrays.asList(remainCountCacheKey, remainCountCacheKey), Arrays.asList(String.valueOf(count), String.valueOf(zeroCountExpireTime)));
        int res = Parser.parseInt(result.toString(), CacheErrno.NO_CACHE);
        if(res > 0){
            m_cache.expire(remainCountCacheKey, m_cache.getExpireSecond());
        }
        return res;
    }
    public static int makeupRemainCount(int aid, int unionPriId, long skuId, int count){
        count = -count;
        String remainCountCacheKey = getRemainCountCacheKey(aid, unionPriId, skuId);
        RedisClient redisClient = m_cache.getRedisClient();
        Object result = redisClient.eval(LuaScript.REDUCE_REMAIN_COUNT, Arrays.asList(remainCountCacheKey, remainCountCacheKey), Arrays.asList(String.valueOf(count), String.valueOf(0)));
        int res = Parser.parseInt(result.toString(), CacheErrno.NO_CACHE);
        if(res > 0){
            m_cache.expire(remainCountCacheKey, m_cache.getExpireSecond());
        }
        return res;
    }

    public static boolean delRemainCount(int aid, int unionPriId, long skuId) {
        String remainCountCacheKey = getRemainCountCacheKey(aid, unionPriId, skuId);
        boolean boo = m_cache.del(remainCountCacheKey);
        return boo;
    }
    public static boolean delRemainCount(int aid, Set<SkuBizKey> skuBizKeySet) {
        if(skuBizKeySet.isEmpty()){
            return true;
        }
        String[] keys = new String[skuBizKeySet.size()];
        int i = 0;
        for (SkuBizKey skuBizKey : skuBizKeySet) {
            int unionPriId = skuBizKey.unionPriId;
            long skuId  = skuBizKey.skuId;
            keys[i++] = getRemainCountCacheKey(aid, unionPriId, skuId);
        }
        boolean boo = m_cache.del(keys);
        return boo;
    }


    public static String getCacheKey(int aid, int unionPriId, int pdId) {
        return wrapCacheVersion(CACHE_KEY_PREFIX + ":" + aid + "-" + unionPriId + "-" + pdId, aid);
    }

    public static String getRemainCountCacheKey(int aid, int unionPriId, long skuId){
        return wrapCacheVersion(CACHE_KEY_PREFIX + "_remainCount:" + aid + "-" + unionPriId + "-" + skuId, aid);
    }

    private static final String CACHE_KEY_PREFIX = "MG_storeSalesSku";

    /**
     * lua 脚本
     */
    private static final class LuaScript{
        /**
         * 原子扣减库存
         * return:
         *  >= 0 扣减成功
         *  CacheErrno.NO_CACHE 不存在缓存
         *  CacheErrno.SHORTAGE 库存不足
         */
        public static final String REDUCE_REMAIN_COUNT =
                "if (redis.call('exists', KEYS[1]) == 1) then " +
                "    local remainCount = tonumber(redis.call('get', KEYS[1])); " +
                "    local num = tonumber(ARGV[1]); " +
                "    if (remainCount >= num) then " +
                "        remainCount = tonumber(redis.call('incrby', KEYS[1], 0 - num));" +
                "        if(remainCount == 0) then " +
                "           redis.call('expire', KEYS[2], ARGV[2]); "+
                "        end; " +
                "        return remainCount; " +
                "    end;" +
                "    return "+CacheErrno.SHORTAGE+";" +
                "end;" +
                "return "+ CacheErrno.NO_CACHE +";";

    }

    public static final class CacheErrno{
        public static final int NO_CACHE = -1;
        public static final int SHORTAGE = -2;
    }
}
