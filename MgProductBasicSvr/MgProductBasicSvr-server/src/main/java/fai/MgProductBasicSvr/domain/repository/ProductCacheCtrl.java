package fai.MgProductBasicSvr.domain.repository;

import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.interfaces.dto.ProductDto;
import fai.comm.util.*;

import java.util.List;

public class ProductCacheCtrl extends CacheCtrl {

    public static Param getCacheInfo(int aid, int pdId) {
        String cacheKey = getCacheKey(aid);
        return m_cache.hgetParam(cacheKey, pdId, ProductDto.Key.INFO, ProductDto.getInfoDto());
    }

    public static FaiList<Param> getCacheList(int aid, List<String> pdIds) {
        String cacheKey = getCacheKey(aid);
        FaiList<Param> list = null;
        try {
            list = m_cache.hmget(cacheKey, ProductDto.Key.INFO, ProductDto.getInfoDto(), pdIds);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;pdIds=%s;", aid, pdIds);
        }
        return list;
    }

    public static void addCache(int aid, Param info) {
        if(Str.isEmpty(info)) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        int pdId = info.getInt(ProductEntity.Info.PD_ID);
        m_cache.hsetParam(true, cacheKey, String.valueOf(pdId), info, ProductDto.Key.INFO, ProductDto.getInfoDto());
        // 更新数量缓存
        Integer count = getCountCache(aid);
        if(count != null) {
            setCountCache(aid, count++);
        }
    }

    public static void addCacheList(int aid, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        m_cache.hmsetFaiList(cacheKey, ProductEntity.Info.PD_ID, Var.Type.INT, list, ProductDto.Key.INFO, ProductDto.getInfoDto());
        // 更新数量缓存
        Integer count = getCountCache(aid);
        if(count != null) {
            count += list.size();
            setCountCache(aid, count);
        }
    }

    public static void delCache(int aid) {
        String cacheKey = getCacheKey(aid);
        m_cache.del(cacheKey);
    }

    public static void delCacheList(int aid, FaiList<Integer> pdIds) {
        if(pdIds == null || pdIds.isEmpty()) {
            return;
        }
        String[] pdIdStrs = new String[pdIds.size()];
        for(int i = 0; i < pdIds.size(); i++) {
            pdIdStrs[i] = String.valueOf(pdIds.get(i));
        }
        String cacheKey = getCacheKey(aid);
        m_cache.hdel(cacheKey, pdIdStrs);
        // 更新数量缓存
        Integer count = getCountCache(aid);
        if(count != null) {
            count -= pdIds.size();
            setCountCache(aid, count);
        }
    }

    public static void setExpire(int aid) {
        String cacheKey = getCacheKey(aid);
        m_cache.expire(cacheKey, EXPIRE_SECOND);
    }

    public static String getCacheKey(int aid) {
        return CACHE_KEY + "-" + aid;
    }

    /** productRel Count cache **/

    public static void setCountCache(int aid, int count) {
        String cacheKey = getCountCacheKey(aid);
        m_cache.set(cacheKey, String.valueOf(count));
    }

    public static Integer getCountCache(int aid) {
        String cacheKey = getCountCacheKey(aid);
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

    public static String getCountCacheKey(int aid) {
        return COUNT_CACHE_KEY + "-" + aid;
    }

    private static final int EXPIRE_SECOND = 10;
    private static final String CACHE_KEY = "MG_product";
    private static final String COUNT_CACHE_KEY = "MG_productCount";// 商品数据量缓存，aid+unionPriId 做 cache key
}
