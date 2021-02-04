package fai.MgProductStoreSvr.domain.repository;

import fai.MgProductStoreSvr.domain.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.interfaces.dto.SpuBizSummaryDto;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Var;

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
    public static void appendCacheList(int aid, int unionPriId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId);
        boolean boo = m_cache.hmsetFaiList(cacheKey, SpuBizSummaryEntity.Info.PD_ID, Var.Type.INT, list, SpuBizSummaryDto.Key.INFO, SpuBizSummaryDto.getInfoDto());
        if(!boo){
            Log.logErr("hmsetFaiList err key:"+cacheKey);
        }
    }

    public static void setCacheDirty(int aid, Set<Integer> unionPriIdSet){
        if(unionPriIdSet == null){
            return;
        }
        for (Integer unionPriId : unionPriIdSet) {
            setCacheDirty(aid, unionPriId);
        }
    }


    public static void setCacheDirty(int aid, int unionPriId){
        String cacheKey = getCacheKey(aid, unionPriId);
        m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND);
    }

    public static String getCacheKey(int aid, int unionPriId) {
        return CACHE_KEY_PREFIX + ":" + aid + "-" + unionPriId;
    }

    private static final String CACHE_KEY_PREFIX = "MG_bizSalesSummary";
}
