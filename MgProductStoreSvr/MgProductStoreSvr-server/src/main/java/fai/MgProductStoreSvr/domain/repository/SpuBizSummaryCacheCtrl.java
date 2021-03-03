package fai.MgProductStoreSvr.domain.repository;

import fai.MgProductStoreSvr.domain.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.interfaces.dto.SpuBizSummaryDto;
import fai.comm.util.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SpuBizSummaryCacheCtrl extends CacheCtrl{

    public static FaiList<Param> getCacheList(int aid, int unionPriId, FaiList<Integer> pdIdList) {
        String cacheKey = getCacheKey(aid, unionPriId);
        List<String> pdIdStrList = pdIdList.stream().map(String::valueOf).collect(Collectors.toList());
        try {
            return m_cache.hmget(cacheKey, SpuBizSummaryDto.Key.INFO, SpuBizSummaryDto.getInfoDto(), pdIdStrList);
        } catch (Exception e) {
            Log.logErr(e, "getCacheList err;aid=%s;unionPriId=%s;pdIdList=%s;", aid, unionPriId, pdIdList);
        }
        return null;
    }

    public static void delCache(int aid, Map<Integer, FaiList<Integer>> unionPirIdPdIdListMap) {
        unionPirIdPdIdListMap.forEach((unionPirId, pdIdList)->{
            String cacheKey = getCacheKey(aid, unionPirId);
            List<String> pdIdStrList = pdIdList.stream().map(String::valueOf).collect(Collectors.toList());
            boolean boo = m_cache.hdel(cacheKey, pdIdStrList.toArray(new String[]{}));
            if(!boo){
                Log.logErr("hdel err key:"+cacheKey+"fields:"+ pdIdList);
            }
        });
    }
    public static void delCache(int aid, int unionPriId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        if(m_cache.exists(cacheKey)) {
            boolean boo = m_cache.del(cacheKey);
            if(!boo){
                Log.logErr("hdel err key:"+cacheKey);
            }
        }
    }
    public static void setCacheList(int aid, int unionPriId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId);
        boolean boo = m_cache.hmsetFaiList(cacheKey, SpuBizSummaryEntity.Info.PD_ID, Var.Type.INT, list, SpuBizSummaryDto.Key.INFO, SpuBizSummaryDto.getInfoDto());
        if(!boo){
            Log.logErr("hmsetFaiList err key:"+cacheKey);
        }
    }
    public static int getTotal(int aid, int unionPriId){
        String totalCacheKey = getTotalCacheKey(aid, unionPriId);
        String totalStr = m_cache.get(totalCacheKey);
        int total = Parser.parseInt(totalStr, -1);
        return total;
    }

    public static boolean setTotal(int aid, int unionPriId, int count) {
        String totalCacheKey = getTotalCacheKey(aid, unionPriId);
        return m_cache.set(totalCacheKey, String.valueOf(count));
    }

    public static boolean setCacheDirty(int aid, Set<Integer> unionPriIdSet){
        if(unionPriIdSet == null){
            return true;
        }
        boolean boo = true;
        for (Integer unionPriId : unionPriIdSet) {
            boo &= setCacheDirty(aid, unionPriId);
        }
        return boo;
    }

    public static boolean setCacheDirty(int aid, int unionPriId){
        String cacheKey = getCacheKey(aid, unionPriId);
        return m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND, DIRTY_EXPIRE_SECOND_RANDOM);
    }

    public static boolean setTotalCacheDirty(int aid, Set<Integer> unionPriIdSet){
        if(unionPriIdSet == null){
            return true;
        }
        boolean boo = true;
        for (Integer unionPriId : unionPriIdSet) {
            boo &= setTotalCacheDirty(aid, unionPriId);
        }
        return boo;
    }

    public static boolean setTotalCacheDirty(int aid, int unionPriId){
        String cacheKey = getTotalCacheKey(aid, unionPriId);
        return m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND, DIRTY_EXPIRE_SECOND_RANDOM);
    }
    protected static String getTotalCacheKey(int aid, int unionPriId){
        return CACHE_KEY_PREFIX + "-count:" + aid + "-" + unionPriId;
    }

    protected static String getCacheKey(int aid, int unionPriId) {
        return CACHE_KEY_PREFIX + ":" + aid + "-" + unionPriId;
    }

    /**
     * 最后一次更新缓存
     */
    public static final class LastUpdateCache {
        /**
         * 获取最后一次更新数据时间
         */
        public static Long getLastUpdateTime(DataType dataType, int aid, int unionPriId){
            String lastUpdateTimeCacheKey = getLastUpdateTimeCacheKey(dataType, aid, unionPriId);
            String lastUpdateTimeStr = m_cache.get(lastUpdateTimeCacheKey);
            if(lastUpdateTimeStr == null){
                return null;
            }
            // 有访问就更新过期时间
            m_cache.expire(lastUpdateTimeCacheKey, m_cache.getExpireSecond(), m_cache.getExpireRandomSecond());
            return Long.parseLong(lastUpdateTimeStr);
        }

        /**
         * 设置最后一次更新数据时间
         */
        public static boolean setLastUpdateTime(DataType dataType, int aid, int unionPriId, long lastUpdateTime){
            String lastUpdateTimeCacheKey = getLastUpdateTimeCacheKey(dataType, aid, unionPriId);
            return m_cache.set(lastUpdateTimeCacheKey, String.valueOf(lastUpdateTime));
        }

        /**
         * 访客态数据最后一次修改时间key
         */
        protected static String getLastUpdateTimeCacheKey(DataType dataType, int aid, int unionPriId){
            return CACHE_KEY_PREFIX + "-"+dataType.getLabel()+":"+ aid + "-" + unionPriId;
        }

    }

    private static final String CACHE_KEY_PREFIX = "MG_spuBizSummary";
}
