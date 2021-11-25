package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindGroupDto;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.misc.Utils;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductBindGroupCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, FaiList<Integer> pdIds, Ref<FaiList<Integer>> nocacheIdsRef) {
        FaiList<Param> list = new FaiList<>();
        if(Utils.isEmptyList(pdIds)) {
            return list;
        }
        FaiList<String> cacheKeys = new FaiList<>();
        for(Integer pdId : pdIds) {
            cacheKeys.add(getCacheKey(aid, unionPriId, pdId));
        }
        try {
            list = getFaiList(cacheKeys, ProductBindGroupDto.getInfoDto(), ProductBindGroupDto.Key.INFO);
            if(list == null) {
                list = new FaiList<>();
            }
            list.remove(null);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;unionPriId=%d;pdIds=%s;", aid, unionPriId, pdIds);
        }

        // 返回没有缓存的id
        FaiList<Integer> noCacheIds = new FaiList<>();
        noCacheIds.addAll(pdIds);
        for(Param info : list) {
            Integer pdId = info.getInt(ProductBindGroupEntity.Info.PD_ID);
            noCacheIds.remove(pdId);
        }
        nocacheIdsRef.value = EmptyCache.getNoCacheIds(aid, unionPriId, noCacheIds);

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

    public static String getCacheKey(int aid, int unionPriId, int pdId) {
        return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + pdId, aid);
    }

    private static final String CACHE_KEY = "MG_productBindGroup";

    /** 空数据缓存 **/
    public static class EmptyCache {
        public static FaiList<Integer> getNoCacheIds(int aid, int unionPriId, FaiList<Integer> ids) {
            if(Utils.isEmptyList(ids)) {
                return null;
            }
            String cacheKey = getEmptyCacheKey(aid, unionPriId);
            List<Integer> noCacheIds = EmptyCacheManager.getNotExisted(cacheKey, ids);
            return new FaiList<>(noCacheIds);
        }

        /*public static FaiList<Param> getCacheList(int aid, int unionPriId, List<String> pdIds) {
            String cacheKey = getEmptyCacheKey(aid, unionPriId);
            FaiList<Param> list = null;
            try {
                list = m_cache.hmget(cacheKey, ProductBindGroupDto.Key.INFO, ProductBindGroupDto.getInfoDto(), pdIds);
            } catch (Exception e) {
                Log.logErr(e,"getCacheList error;aid=%d;pdIds=%s;", aid, pdIds);
            }
            return list;
        }*/

        public static void addCacheList(int aid, int unionPriId, FaiList<Integer> pdIds) {
            String cacheKey = getEmptyCacheKey(aid, unionPriId);
            EmptyCacheManager.addCacheList(cacheKey, pdIds);
        }

        public static void delCache(int aid, int unionPriId, int pdId) {
            EmptyCacheManager.delCache(getEmptyCacheKey(aid, unionPriId), pdId);
        }

        public static void delCache(int aid, int unionPriId, FaiList<Integer> pdIds) {
            EmptyCacheManager.delCache(getEmptyCacheKey(aid, unionPriId), pdIds);
        }

        public static String getEmptyCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(EMPTY_CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }

        private static final String EMPTY_CACHE_KEY = "MG_pdBdGroupEmpty";
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
}
