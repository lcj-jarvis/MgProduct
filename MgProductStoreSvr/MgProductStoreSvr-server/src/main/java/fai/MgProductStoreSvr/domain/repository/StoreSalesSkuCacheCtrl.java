package fai.MgProductStoreSvr.domain.repository;

import fai.MgProductStoreSvr.interfaces.dto.StoreSalesSkuDto;
import fai.comm.util.FaiList;
import fai.comm.util.Param;

public class StoreSalesSkuCacheCtrl extends CacheCtrl{

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


    public static String getCacheKey(int aid, int unionPriId, int pdId) {
        return CACHE_KEY_PREFIX + ":" + aid + "-" + unionPriId + "-" + pdId;
    }

    private static final String CACHE_KEY_PREFIX = "MG_storeSalesSku";
}
