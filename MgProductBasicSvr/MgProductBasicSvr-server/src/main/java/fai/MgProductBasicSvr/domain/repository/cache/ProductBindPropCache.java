package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.comm.util.FaiList;
import fai.comm.util.Param;

public class ProductBindPropCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, int rlPdId) {
        String cacheKey = getCacheKey(aid, unionPriId, rlPdId);
        return m_cache.getFaiList(cacheKey, ProductBindPropDto.Key.INFO, ProductBindPropDto.getInfoDto());
    }

    public static void setCacheList(int aid, int unionPriId, int rlPdId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId, rlPdId);
        m_cache.setFaiList(cacheKey, list, ProductBindPropDto.Key.INFO, ProductBindPropDto.getInfoDto());
    }

    public static void delCache(int aid, int unionPriId, int rlPdId) {
        String cacheKey = getCacheKey(aid, unionPriId, rlPdId);
        if(m_cache.exists(cacheKey)) {
            m_cache.del(cacheKey);
        }
    }

    public static String getCacheKey(int aid, int unionPriId, int rlPdId) {
        return CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + rlPdId;
    }

    private static final int EXPIRE_SECOND = 10;
    private static final String CACHE_KEY = "MG_productBindProp";
}
