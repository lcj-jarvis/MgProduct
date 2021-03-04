package fai.MgProductStoreSvr.domain.repository;

import fai.MgProductStoreSvr.domain.entity.SpuSummaryEntity;
import fai.MgProductStoreSvr.interfaces.dto.SpuSummaryDto;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Var;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SpuSummaryCacheCtrl  extends CacheCtrl{

    public static FaiList<Param> getCacheList(int aid, Set<Integer> pdIdSet) {
        if(pdIdSet.isEmpty()){
            return new FaiList<>();
        }
        String cacheKey = getCacheKey(aid);
        List<String> pdIdStrList = pdIdSet.stream().map(String::valueOf).collect(Collectors.toList());
        try {
            return m_cache.hmget(cacheKey, SpuSummaryDto.Key.INFO, SpuSummaryDto.getInfoDto(), pdIdStrList);
        } catch (Exception e) {
            Log.logErr(e, "getCacheList err;aid=%s;pdIdSet=%s;", aid, pdIdSet);
        }
        return null;
    }

    public static boolean setCacheList(int aid, FaiList<Param> infoList) {
        if(infoList.isEmpty()){
            return true;
        }
        String cacheKey = getCacheKey(aid);
        return m_cache.hmsetFaiList(cacheKey, SpuSummaryEntity.Info.PD_ID, Var.Type.INT, infoList, SpuSummaryDto.Key.INFO, SpuSummaryDto.getInfoDto());
    }

    public static boolean delCacheList(int aid, Set<Integer> pdIdSet) {
        if(pdIdSet.isEmpty()){
            return true;
        }
        String cacheKey = getCacheKey(aid);
        List<String> pdIdStrList = pdIdSet.stream().map(String::valueOf).collect(Collectors.toList());
        return m_cache.hdel(cacheKey, pdIdStrList.toArray(new String[]{}));
    }

    public static boolean setCacheDirty(int aid) {
        String cacheKey = getCacheKey(aid);
        return m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND, DIRTY_EXPIRE_SECOND_RANDOM);
    }

    protected static String getCacheKey(int aid) {
        return CACHE_KEY_PREFIX + ":" + aid;
    }

    private static final String CACHE_KEY_PREFIX = "MG_spuSummary";


}
