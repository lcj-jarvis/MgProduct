package fai.MgProductBasicSvr.domain.repository;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.interfaces.dto.ProductDto;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.comm.util.*;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductRelCacheCtrl extends CacheCtrl {

    public static boolean exist(int aid, int unionPriId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        return m_cache.exists(cacheKey);
    }

    public static Param getCacheInfo(int aid, int unionPriId, int rlPdId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        return m_cache.hgetParam(cacheKey, rlPdId, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
    }

    public static FaiList<Param> getCacheList(int aid, int unionPriId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        return m_cache.hgetAllFaiList(cacheKey, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
    }

    public static FaiList<Param> getCacheList(int aid, int unionPriId, List<String> rlPdIds) {
        String cacheKey = getCacheKey(aid, unionPriId);
        FaiList<Param> list = null;
        try {
            list = m_cache.hmget(cacheKey, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto(), rlPdIds);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;rlPdIds=%s;", aid, rlPdIds);
        }
        return list;
    }

    public static void delCache(int aid, int unionPriId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        m_cache.del(cacheKey);
    }

    public static void delCacheByUids(int aid, HashSet<Integer> unionPriIds) {
        String[] cacheKeys = getCacheKeys(aid, unionPriIds);
        if(cacheKeys == null) {
            return;
        }
        m_cache.del(cacheKeys);
    }

    public static void delCache(int aid, int unionPriId, int rlPdId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        m_cache.hdel(cacheKey, String.valueOf(rlPdId));
        // 更新数量缓存
        Integer count = getRelCountCache(aid, unionPriId);
        if(count != null) {
            setRelCountCache(aid, unionPriId, count--);
        }
    }

    public static void delCacheList(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            return;
        }
        String[] rlPdIdStrs = new String[rlPdIds.size()];
        for(int i = 0; i < rlPdIds.size(); i++) {
            rlPdIdStrs[i] = String.valueOf(rlPdIds.get(i));
        }
        String cacheKey = getCacheKey(aid, unionPriId);
        m_cache.hdel(cacheKey, rlPdIdStrs);
        // 更新数量缓存
        Integer count = getRelCountCache(aid, unionPriId);
        if(count != null) {
            count -= rlPdIdStrs.length;
            setRelCountCache(aid, unionPriId, count);
        }
    }


    public static void addCacheList(int aid, int unionPriId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId);
        m_cache.hmsetFaiList(cacheKey, ProductRelEntity.Info.RL_PD_ID, Var.Type.INT, list, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
        // 更新数量缓存
        Integer count = getRelCountCache(aid, unionPriId);
        if(count != null) {
            count += list.size();
            setRelCountCache(aid, unionPriId, count);
        }
    }

    public static void addCache(int aid, int unionPriId, Param info) {
        if(Str.isEmpty(info)) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId);
        int rlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
        m_cache.hsetParam(true, cacheKey, String.valueOf(rlPdId), info, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
        // 更新数量缓存
        Integer count = getRelCountCache(aid, unionPriId);
        if(count != null) {
            setRelCountCache(aid, unionPriId, count++);
        }
    }

    public static void setExpire(int aid, int uninoId) {
        String cacheKey = getCacheKey(aid, uninoId);
        m_cache.expire(cacheKey, EXPIRE_SECOND);
    }

    public static String getCacheKey(int aid, int unionPriId) {
        return CACHE_KEY + "-" + aid + "-" + unionPriId;
    }

    public static String[] getCacheKeys(int aid, HashSet<Integer> unionPriIds) {
        if(unionPriIds == null || unionPriIds.isEmpty()) {
            return null;
        }
        String[] keys = new String[unionPriIds.size()];
        int index = 0;
        for(int unionPriId : unionPriIds) {
            keys[index++] = getCacheKey(aid, unionPriId);
        }
        return keys;
    }

    /** cache: aid+unionPriId+pdId -> rlPdId **/
    public static void addRlIdRelCacheList(int aid, int unionPriId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getRlIdRelCacheKey(aid, unionPriId);
        m_cache.hmsetFaiList(cacheKey, ProductRelEntity.Info.RL_PD_ID, Var.Type.INT, list, ProductRelDto.Key.REDUCED_INFO, ProductRelDto.getReducedInfoDto());
    }

    public static FaiList<Param> getRlIdRelCacheList(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        FaiList list = null;
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            return list;
        }
        List<String> rlPdIdStrs = rlPdIds.stream().map(String::valueOf).collect(Collectors.toList());
        String cacheKey = getRlIdRelCacheKey(aid, unionPriId);
        try {
            list = m_cache.hmget(cacheKey, ProductRelDto.Key.REDUCED_INFO, ProductRelDto.getReducedInfoDto(), rlPdIdStrs);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;rlPdIdStrs=%s;", aid, rlPdIdStrs);
        }
        return list;
    }

    public static void delRlIdRelCache(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            return;
        }
        String[] rlPdIdStrs = new String[rlPdIds.size()];
        for(int i = 0; i < rlPdIds.size(); i++) {
            rlPdIdStrs[i] = String.valueOf(rlPdIds.get(i));
        }
        String cacheKey = getRlIdRelCacheKey(aid, unionPriId);
        m_cache.hdel(cacheKey, rlPdIdStrs);
    }

    public static String getRlIdRelCacheKey(int aid, int unionPriId) {
        return RLID_REL_CACHE_KEY + "-" + aid + "-" + unionPriId;
    }

    /** productRel Count cache **/

    public static void setRelCountCache(int aid, int unionPriId, int count) {
        String cacheKey = getRelCountCacheKey(aid, unionPriId);
        m_cache.set(cacheKey, String.valueOf(count));
    }

    public static Integer getRelCountCache(int aid, int unionPriId) {
        String cacheKey = getRelCountCacheKey(aid, unionPriId);
        String countStr = m_cache.get(cacheKey);
        if(Str.isEmpty(countStr)) {
            return null;
        }
        int count = Parser.parseInt(countStr, -1);
        if(count < 0) {
            return null;
        }
        return count;
    }

    public static String getRelCountCacheKey(int aid, int unionPriId) {
        return COUNT_CACHE_KEY + "-" + aid + "-" + unionPriId;
    }

    private static final int EXPIRE_SECOND = 10;
    private static final String CACHE_KEY = "MG_productRel"; // 商品业务表数据缓存，aid+unionPriId 做 cache key，rlPdId做hash key
    private static final String RLID_REL_CACHE_KEY = "MG_productRlIdRel"; // 商品业务表 id和业务id关系缓存，aid + unionPriId 为 cache key，pdId为hash key，数据只包含rlPdId+pdId+unionPriId
    private static final String COUNT_CACHE_KEY = "MG_productRelCount";// 商品业务表数据量缓存，aid+unionPriId 做 cache key
}
