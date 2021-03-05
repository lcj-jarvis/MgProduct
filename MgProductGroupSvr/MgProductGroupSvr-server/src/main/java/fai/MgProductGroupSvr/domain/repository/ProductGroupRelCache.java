package fai.MgProductGroupSvr.domain.repository;

import fai.MgProductGroupSvr.domain.entity.ProductGroupRelEntity;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupRelDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

import java.util.Calendar;

public class ProductGroupRelCache extends CacheCtrl {
    public static FaiList<Param> getCacheList(int aid, int unionPriId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        return m_cache.hgetAllFaiList(cacheKey, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getInfoDto());
    }

    public static void addCache(int aid, int uninoId, Param info) {
        if(Str.isEmpty(info)) {
            return;
        }
        String cacheKey = getCacheKey(aid, uninoId);
        int rlGroupId = info.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 0);
        m_cache.hsetParam(true, cacheKey, String.valueOf(rlGroupId), info, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getInfoDto());
    }

    public static void addCacheList(int aid, int uninoId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, uninoId);
        m_cache.hmsetFaiList(cacheKey, ProductGroupRelEntity.Info.RL_GROUP_ID, Var.Type.INT, list, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getInfoDto());
    }

    public static void delCacheList(int aid, int uninoId, FaiList<Integer> rlGroupIds) {
        if(rlGroupIds == null || rlGroupIds.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, uninoId);
        if(!m_cache.exists(cacheKey)) {
            return;
        }
        String[] rlGroupIdStrs = new String[rlGroupIds.size()];
        for(int i = 0; i < rlGroupIds.size(); i++) {
            rlGroupIdStrs[i] = String.valueOf(rlGroupIds.get(i));
        }
        m_cache.hdel(cacheKey, rlGroupIdStrs);
    }

    public static void updateCacheList(int aid, int uninoId, FaiList<ParamUpdater> updaterList) {
        if(updaterList == null || updaterList.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, uninoId);
        if(!m_cache.exists(cacheKey)) {
            return;
        }
        for(ParamUpdater updater : updaterList) {
            Param info = updater.getData();
            int rlGroupId = info.getInt(ProductGroupRelEntity.Info.RL_GROUP_ID, 0);
            m_cache.hsetParam(cacheKey, String.valueOf(rlGroupId), updater, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getInfoDto());
        }
    }

    public static boolean exists(int aid, int uninoId) {
        String cacheKey = getCacheKey(aid, uninoId);
        return m_cache.exists(cacheKey);
    }

    public static void setExpire(int aid, int uninoId) {
        String cacheKey = getCacheKey(aid, uninoId);
        m_cache.expire(cacheKey, EXPIRE_SECOND);
    }

    public static String getCacheKey(int aid, int unionPriId) {
        return CACHE_KEY + "-" + aid + "-" + unionPriId;
    }

    private static final int EXPIRE_SECOND = 10;
    private static final String CACHE_KEY = "MG_productGroupRel";

    /*** sort 字段的 cache ***/
    public static class SortCache {
        public static void set(int aid, int unionPriId, int sort) {
            if(sort < 0) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.set(cacheKey, String.valueOf(sort));
        }

        public static void del(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            if(!m_cache.exists(cacheKey)) {
                return;
            }
            m_cache.del(cacheKey);
        }

        public static String get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            if(!m_cache.exists(cacheKey)) {
                return null;
            }
            return m_cache.get(cacheKey);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return SORT_CACHE_KEY + "-" + aid + "-" + unionPriId;
        }
        private static final String SORT_CACHE_KEY = "MG_productGroupRelSort";
    }

    /** 数据状态缓存 **/
    public static class DataStatusCache {

        public static Param get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.getParam(cacheKey, ProductGroupRelDto.Key.DATA_STATUS, ProductGroupRelDto.getDataStatusDto());
        }

        public static void add(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.setParam(cacheKey, info, ProductGroupRelDto.Key.DATA_STATUS, ProductGroupRelDto.getDataStatusDto());
        }

        public static void update(int aid, int unionPriId) {
            update(aid, unionPriId, 0);
        }

        public static void update(int aid, int unionPriId, int count) {
            update(aid, unionPriId, count, true);
        }

        public static void update(int aid, int unionPriId, int count, boolean add) {
            Param info = new Param();
            info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, System.currentTimeMillis());
            ParamUpdater updater = new ParamUpdater(info);
            if(count != 0) {
                String op = add ? ParamUpdater.INC : ParamUpdater.DEC;
                updater.add(DataStatus.Info.TOTAL_SIZE, op, count);
            }
            m_cache.updateParam(getCacheKey(aid, unionPriId), updater, ProductGroupRelDto.Key.DATA_STATUS, ProductGroupRelDto.getDataStatusDto());
        }

        public static void expire(int aid, int unionPriId, int second) {
            m_cache.expire(getCacheKey(aid, unionPriId), second);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return DATA_STATUS_CACHE_KEY + "-" + aid + "-" + unionPriId;
        }

        private static final String DATA_STATUS_CACHE_KEY = "MG_productGroupRelDataStatus";
    }
}
