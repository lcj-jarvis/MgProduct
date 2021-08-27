package fai.MgProductGroupSvr.domain.repository;

import fai.MgProductGroupSvr.domain.entity.ProductGroupRelEntity;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupRelDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

public class ProductGroupRelCache extends CacheCtrl {

    public static class InfoCache {
        public static FaiList<Param> getCacheList(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.hgetAllFaiList(cacheKey, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getInfoDto());
        }

        public static void addCache(int aid, int unionId, Param info) {
            if(Str.isEmpty(info)) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            int groupId = info.getInt(ProductGroupRelEntity.Info.GROUP_ID, 0);
            m_cache.hsetParam(true, cacheKey, String.valueOf(groupId), info, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getInfoDto());
        }

        public static void addCacheList(int aid, int unionId, FaiList<Param> list) {
            if(list == null || list.isEmpty()) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            m_cache.hmsetFaiList(cacheKey, ProductGroupRelEntity.Info.GROUP_ID, Var.Type.INT, list, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getInfoDto());
        }

        public static void delCacheList(int aid, int unionId, FaiList<Integer> groupIds) {
            if(groupIds == null || groupIds.isEmpty()) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            if(!m_cache.exists(cacheKey)) {
                return;
            }
            String[] groupIdStrs = new String[groupIds.size()];
            for(int i = 0; i < groupIds.size(); i++) {
                groupIdStrs[i] = String.valueOf(groupIds.get(i));
            }
            m_cache.hdel(cacheKey, groupIdStrs);
        }

        public static void delCache(int aid, int unionId) {
            m_cache.del(getCacheKey(aid, unionId));
        }

        public static void updateCacheList(int aid, int unionId, FaiList<ParamUpdater> updaterList) {
            if(updaterList == null || updaterList.isEmpty()) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            if(!m_cache.exists(cacheKey)) {
                return;
            }
            for(ParamUpdater updater : updaterList) {
                Param info = updater.getData();
                int groupId = info.getInt(ProductGroupRelEntity.Info.GROUP_ID, 0);
                m_cache.hsetParam(cacheKey, String.valueOf(groupId), updater, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getInfoDto());
            }
        }

        public static boolean exists(int aid, int unionId) {
            String cacheKey = getCacheKey(aid, unionId);
            return m_cache.exists(cacheKey);
        }

        public static void setExpire(int aid, int unionId) {
            String cacheKey = getCacheKey(aid, unionId);
            m_cache.expire(cacheKey, EXPIRE_SECOND);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }

        private static final String CACHE_KEY = "MG_productGroupRel";
    }
    private static final int EXPIRE_SECOND = 10;

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
            return wrapCacheVersion(SORT_CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }
        private static final String SORT_CACHE_KEY = "MG_productGroupRelSort";
    }

    /** 数据状态缓存 **/
    public static class DataStatusCache {

        public static Param get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.getParam(cacheKey, ProductGroupRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void add(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.setParam(cacheKey, info, ProductGroupRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
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
            m_cache.updateParam(getCacheKey(aid, unionPriId), updater, ProductGroupRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void expire(int aid, int unionPriId, int second) {
            m_cache.expire(getCacheKey(aid, unionPriId), second);
        }

        public static void delCache(int aid, int unionPriId) {
            m_cache.del(getCacheKey(aid, unionPriId));
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(DATA_STATUS_CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }

        private static final String DATA_STATUS_CACHE_KEY = "MG_pdGroupRelDS";
    }
}
