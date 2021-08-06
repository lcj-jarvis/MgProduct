package fai.MgProductSearchSvr.domain.repository.cache;

import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.util.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author LuChaoJi
 * @date 2021-08-06 10:28
 */
public class MgProductSearchCache {

    public static void init(RedisCacheManager cache, ParamCacheRecycle cacheRecycle){
        m_result_cache = cache;
        m_cacheRecycle = cacheRecycle;
        m_cacheRecycle.addParamCache("localDataStatusCache", m_localDataStatusCache);
    }

    /**
     * 本地缓存回收器
     */
    private static ParamCacheRecycle m_cacheRecycle;

    /**
     * 各个表的本地数据缓存
     * ConcurrentHashMap<Integer, ParamListCache1>  eg: <unionPriId, ParamCache1>
     */
    private static final ConcurrentHashMap<Integer, ParamListCache1> m_localMgProductSearchDataCache = new ConcurrentHashMap<>();
    public static String getLocalMgProductSearchDataCacheKey(int aid, String searchTableName){
        return aid + "-" + searchTableName;
    }

    public static ParamListCache1 getLocalMgProductSearchDataCache(int unionPriId) {
        ParamListCache1 cache = m_localMgProductSearchDataCache.get(unionPriId);
        if (cache != null) {
            return cache;
        }
        synchronized (m_localMgProductSearchDataCache) {
            // double check
            cache = m_localMgProductSearchDataCache.get(unionPriId);
            if (cache != null) {
                return cache;
            }
            cache = new ParamListCache1();
            m_localMgProductSearchDataCache.put(unionPriId, cache);
            m_cacheRecycle.addParamCache("mgpd-" + unionPriId, cache);
            return cache;
        }
    }


    /**
     * 数据的更新时间和总条数的缓存
     */
    private static final ParamCache1 m_localDataStatusCache = new ParamCache1();
    public static class LocalDataStatusCache {
        public static String getDataStatusCacheKey(int aid, int unionPriId, String searchTableName) {
            return aid + "-" + unionPriId + "-" + searchTableName;
        }

        public static Param getLocalDataStatusCache(String cacheKey) {
            return m_localDataStatusCache.get(cacheKey);
        }

        public static void addLocalDataStatusCache(String cacheKey, Param dataStatusInfo) {
            m_localDataStatusCache.put(cacheKey, dataStatusInfo);
        }
    }


    /**
     * 搜索结果集的缓存
     */
    private static RedisCacheManager m_result_cache;
    public static class ResultCache {
        public static String getResultCacheKey(int aid, int unionPriId, String searchParamString){
            // 根据搜索词的 md5
            return aid + "-" + unionPriId + "-" + MD5Util.MD5Encode(searchParamString, "utf-8");
        }

        // Param resultCacheInfo = m_result_cache.getParam(resultCacheKey, MgProductSearchDto.Key.RESULT_INFO,
        //                          MgProductSearchDto.getProductSearchDto());
        public static Param  getCacheInfo(String resultCacheKey) {
            return m_result_cache.getParam(resultCacheKey, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
        }

        public static void delCache(String resultCacheKey) {
            m_result_cache.del(resultCacheKey);
        }

        public static void addCacheInfo(String resultCacheKey, Param resultCacheInfo) {
            m_result_cache.setParam(resultCacheKey, resultCacheInfo, MgProductSearchDto.Key.RESULT_INFO, MgProductSearchDto.getProductSearchDto());
        }
    }
}
