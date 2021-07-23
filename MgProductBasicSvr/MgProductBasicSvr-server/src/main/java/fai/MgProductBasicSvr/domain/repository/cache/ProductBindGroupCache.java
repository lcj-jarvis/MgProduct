package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.interfaces.dto.ProductBindGroupDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;

import java.util.HashSet;

public class ProductBindGroupCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        FaiList<Param> list = null;
        if(Util.isEmptyList(rlPdIds)) {
            return null;
        }
        FaiList<String> cacheKeys = new FaiList<>();
        for(Integer rlPdId : rlPdIds) {
            cacheKeys.add(getCacheKey(aid, unionPriId, rlPdId));
        }
        try {
            list = getFaiList(cacheKeys);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
        }

        return list;
    }

    public static void addCacheList(int aid, int unionPriId, int rlPdId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId, rlPdId);
        m_cache.setFaiList(cacheKey, list, ProductBindGroupDto.Key.INFO, ProductBindGroupDto.getInfoDto());
    }

    public static void delCacheList(int aid, int unionPriId, FaiList<Integer> rlPdIds) {
        if(Util.isEmptyList(rlPdIds)) {
            return;
        }
        String[] cacheKeys = new String[rlPdIds.size()];
        for(int i = 0; i < rlPdIds.size(); i++) {
            cacheKeys[i] = getCacheKey(aid, unionPriId, rlPdIds.get(i));
        }

        m_cache.del(cacheKeys);
    }

    public static void delCache(int aid, int unionPriId, Integer rlPdId) {
        m_cache.del(getCacheKey(aid, unionPriId, rlPdId));
    }

    /** 数据状态缓存 **/
    public static class DataStatusCache {
        public static Param get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.getParam(cacheKey, ProductBindGroupDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void add(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.setParam(cacheKey, info, ProductBindGroupDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void update(int aid, int unionPriId, int addCount) {
            Param info = new Param();
            info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, System.currentTimeMillis());
            ParamUpdater updater = new ParamUpdater(info);
            if(addCount != 0) {
                updater.add(DataStatus.Info.TOTAL_SIZE, ParamUpdater.INC, addCount);
            }
            m_cache.updateParam(getCacheKey(aid, unionPriId), updater, ProductBindGroupDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
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

        private static final String DATA_STATUS_CACHE_KEY = "MG_pdBindGroupDS";
    }

    public static String getCacheKey(int aid, int unionPriId, int rlPdId) {
        return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + rlPdId, aid);
    }

    private static final String CACHE_KEY = "MG_productBindGroup";
}
