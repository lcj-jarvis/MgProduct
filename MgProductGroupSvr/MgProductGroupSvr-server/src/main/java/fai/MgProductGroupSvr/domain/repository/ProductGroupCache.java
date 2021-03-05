package fai.MgProductGroupSvr.domain.repository;

import fai.MgProductGroupSvr.domain.entity.ProductGroupEntity;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupDto;
import fai.comm.util.*;

public class ProductGroupCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid) {
        String cacheKey = getCacheKey(aid);
        return m_cache.hgetAllFaiList(cacheKey, ProductGroupDto.Key.INFO, ProductGroupDto.getInfoDto());
    }

    public static void delCacheList(int aid, FaiList<Integer> groupIds) {
        if(groupIds == null || groupIds.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        if(!m_cache.exists(cacheKey)) {
            return;
        }
        String[] propIdStrs = new String[groupIds.size()];
        for(int i = 0; i < groupIds.size(); i++) {
            propIdStrs[i] = String.valueOf(groupIds.get(i));
        }
        m_cache.hdel(cacheKey, propIdStrs);
    }

    public static void updateCacheList(int aid, FaiList<ParamUpdater> updaterList) {
        if(updaterList == null || updaterList.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        if(!m_cache.exists(cacheKey)) {
            return;
        }
        for(ParamUpdater updater : updaterList) {
            Param info = updater.getData();
            int propId = info.getInt(ProductGroupEntity.Info.GROUP_ID, 0);
            m_cache.hsetParam(cacheKey, String.valueOf(propId), updater, ProductGroupDto.Key.INFO, ProductGroupDto.getInfoDto());
        }
    }

    public static void addCache(int aid, Param info) {
        if(Str.isEmpty(info)) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        int propId = info.getInt(ProductGroupEntity.Info.GROUP_ID, 0);
        m_cache.hsetParam(true, cacheKey, String.valueOf(propId), info, ProductGroupDto.Key.INFO, ProductGroupDto.getInfoDto());
    }

    public static void addCacheList(int aid, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        m_cache.hmsetFaiList(cacheKey, ProductGroupEntity.Info.GROUP_ID, Var.Type.INT, list, ProductGroupDto.Key.INFO, ProductGroupDto.getInfoDto());
    }

    public static void setExpire(int aid) {
        String cacheKey = getCacheKey(aid);
        m_cache.expire(cacheKey, EXPIRE_SECOND);
    }

    public static String getCacheKey(int aid) {
        return CACHE_KEY + "-" + aid;
    }

    private static final int EXPIRE_SECOND = 10;
    private static final String CACHE_KEY = "MG_productGroup";
}
