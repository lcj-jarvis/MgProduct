package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.interfaces.dto.ProductBindGroupDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.misc.Utils;

import java.util.HashSet;

public class ProductBindGroupCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, FaiList<Integer> pdIds) {
        FaiList<Param> list = null;
        if(Utils.isEmptyList(pdIds)) {
            return null;
        }
        FaiList<String> cacheKeys = new FaiList<>();
        for(Integer pdId : pdIds) {
            cacheKeys.add(getCacheKey(aid, unionPriId, pdId));
        }
        try {
            list = getFaiList(cacheKeys, ProductBindGroupDto.getInfoDto(), ProductBindGroupDto.Key.INFO);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;unionPriId=%d;pdIds=%s;", aid, unionPriId, pdIds);
        }

        return list;
    }

    public static void addCacheList(int aid, int unionPriId, int pdId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId, pdId);
        m_cache.setFaiList(cacheKey, list, ProductBindGroupDto.Key.INFO, ProductBindGroupDto.getInfoDto());
    }

    public static void delCacheList(int aid, int unionPriId, FaiList<Integer> pdIds) {
        if(Utils.isEmptyList(pdIds)) {
            return;
        }
        String[] cacheKeys = new String[pdIds.size()];
        for(int i = 0; i < pdIds.size(); i++) {
            cacheKeys[i] = getCacheKey(aid, unionPriId, pdIds.get(i));
        }

        m_cache.del(cacheKeys);
    }

    public static void delCache(int aid, int unionPriId, Integer pdId) {
        m_cache.del(getCacheKey(aid, unionPriId, pdId));
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

    public static String getCacheKey(int aid, int unionPriId, int pdId) {
        return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + pdId, aid);
    }

    private static final String CACHE_KEY = "MG_productBindGroup";
}
