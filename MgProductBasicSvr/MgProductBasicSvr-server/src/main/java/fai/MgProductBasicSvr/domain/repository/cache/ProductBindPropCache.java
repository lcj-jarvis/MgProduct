package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.misc.Utils;

import java.util.HashSet;

public class ProductBindPropCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, int sysType, int rlPdId) {
        String cacheKey = getCacheKey(aid, unionPriId, sysType, rlPdId);
        return m_cache.getFaiList(cacheKey, ProductBindPropDto.Key.INFO, ProductBindPropDto.getInfoDto());
    }

    public static void setCacheList(int aid, int unionPriId, int sysType, int rlPdId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId, sysType, rlPdId);
        m_cache.setFaiList(cacheKey, list, ProductBindPropDto.Key.INFO, ProductBindPropDto.getInfoDto());
    }

    public static void delCache(int aid, int unionPriId, int sysType, int rlPdId) {
        String cacheKey = getCacheKey(aid, unionPriId, sysType, rlPdId);
        if(m_cache.exists(cacheKey)) {
            m_cache.del(cacheKey);
        }
    }

    public static void delCacheList(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIdList) {
        if(Utils.isEmptyList(rlPdIdList)) {
            return;
        }
        HashSet<Integer> rlPdIds = new HashSet<>(rlPdIdList);
        String[] cacheKeys = new String[rlPdIds.size()];
        int i = 0;
        for(Integer rlPdId : rlPdIds) {
            cacheKeys[i++] = getCacheKey(aid, unionPriId, sysType, rlPdId);
        }
        m_cache.del(cacheKeys);
    }

    public static String getCacheKey(int aid, int unionPriId, int sysType, int rlPdId) {
        return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + sysType + "-" + rlPdId, aid);
    }

    /** 数据状态缓存 **/
    public static class DataStatusCache {
        public static Param get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.getParam(cacheKey, ProductBindPropDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void add(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.setParam(cacheKey, info, ProductBindPropDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void update(int aid, int unionPriId, int addCount) {
            Param info = new Param();
            info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, System.currentTimeMillis());
            ParamUpdater updater = new ParamUpdater(info);
            if(addCount != 0) {
                updater.add(DataStatus.Info.TOTAL_SIZE, ParamUpdater.INC, addCount);
            }
            m_cache.updateParam(getCacheKey(aid, unionPriId), updater, ProductBindPropDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void del(int aid, HashSet<Integer> unionPriIds) {
            String[] cacheKeys = getCacheKeys(aid, unionPriIds);
            if(cacheKeys == null) {
                return;
            }
            m_cache.del(cacheKeys);
        }

        public static void expire(int aid, int unionPriId, int second) {
            m_cache.expire(getCacheKey(aid, unionPriId), second);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(DATA_STATUS_CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
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

        private static final String DATA_STATUS_CACHE_KEY = "MG_pdBindPropDS";
    }

    private static final int EXPIRE_SECOND = 10;
    private static final String CACHE_KEY = "MG_productBindProp";
}
