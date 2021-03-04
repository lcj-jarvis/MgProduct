package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.domain.entity.ProductGroupAssocEntity;
import fai.MgProductBasicSvr.interfaces.dto.ProductGroupAssocDto;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Var;

import java.util.List;

public class ProductGroupAssocCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, List<String> rlPdIds) {
        String cacheKey = getCacheKey(aid, unionPriId);
        FaiList<Param> list = null;
        try {
            m_cache.hmget(cacheKey, ProductGroupAssocDto.Key.INFO, ProductGroupAssocDto.getInfoDto(), rlPdIds);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
        }
        return list;
    }

    public static void addCacheList(int aid, int unionPriId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId);
        m_cache.hmsetFaiList(cacheKey, ProductGroupAssocEntity.Info.RL_GROUP_ID, Var.Type.INT, list, ProductGroupAssocDto.Key.INFO, ProductGroupAssocDto.getInfoDto());
    }

    public static void delCache(int aid, int unionPriId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        if(m_cache.exists(cacheKey)) {
            m_cache.del(cacheKey);
        }
    }

    public static String getCacheKey(int aid, int unionPriId) {
        return CACHE_KEY + "-" + aid + "-" + unionPriId;
    }

    private static final String CACHE_KEY = "MG_productBindGroup";
}
