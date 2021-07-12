package fai.MgProductTagSvr.application.domain.repository.cache;

import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.interfaces.dto.ProductTagRelDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;

/**
 * @author LuChaoJi
 * @date 2021-07-12 14:00
 */
public class ProductTagRelCache extends CacheCtrl{
    
    private static final int EXPIRE_SECOND = 10;

    public static class InfoCache {

        private static final String CACHE_KEY = "MG_productTagRel";
      
        /**
         * 获取缓存的key，格式：MG_productTagRel-aid:HHmmss
         * @return MG_productTagRel-aid:HHmmss
         */
        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }

        /**
         * 添加FaiList<Param>到缓存
         * 缓存的形式：cacheKey rlTagId  Param rlTagId Param ....
         */
        public static void addCacheList(int aid, int unionId, FaiList<Param> list) {
            if(Util.isEmptyList(list)) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            m_cache.hmsetFaiList(cacheKey, ProductTagRelEntity.Info.RL_TAG_ID, Var.Type.INT, list, ProductTagRelDto.Key.INFO, ProductTagRelDto.getInfoDto());
        }

        /**
         * 删除缓存的FaiList<Param>
         */
        public static void delCacheList(int aid, int uninoId, FaiList<Integer> rlTagIds) {
            if(Util.isEmptyList(rlTagIds)) {
                return;
            }
            String cacheKey = getCacheKey(aid, uninoId);
            if(!m_cache.exists(cacheKey)) {
                return;
            }
            String[] rlTagIdStrs = new String[rlTagIds.size()];
            for(int i = 0; i < rlTagIds.size(); i++) {
                rlTagIdStrs[i] = String.valueOf(rlTagIds.get(i));
            }
            m_cache.hdel(cacheKey, rlTagIdStrs);
        }

        /**
         * 更新缓存的FaiList<Param>
         */
        public static void updateCacheList(int aid, int unionId, FaiList<ParamUpdater> updaterList) {
            if(Util.isEmptyList(updaterList)) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            if(!m_cache.exists(cacheKey)) {
                return;
            }
            for(ParamUpdater updater : updaterList) {
                Param info = updater.getData();
                int rlTagId = info.getInt(ProductTagRelEntity.Info.RL_TAG_ID, 0);
                m_cache.hsetParam(cacheKey, String.valueOf(rlTagId), updater, ProductTagRelDto.Key.INFO, ProductTagRelDto.getInfoDto());
            }
        }

        /**
         * 获取所有的缓存的FaiList<Param> ---> cacheKey:FaiList<Param>
         */
        public static FaiList<Param> getCacheList(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.hgetAllFaiList(cacheKey, ProductTagRelDto.Key.INFO, ProductTagRelDto.getInfoDto());
        }

        /**
         * 如果keyExists为true，执行hset操作，添加单个缓存到hash中。
         *    缓存的形式：cacheKey TagId（byte[]数组的形式） Param（byte[]数组的形式）
         */
        public static void addCache(int aid, int unionId, Param info) {
            if(Str.isEmpty(info)) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            int rlTagId = info.getInt(ProductTagRelEntity.Info.RL_TAG_ID, 0);
            m_cache.hsetParam(true, cacheKey, String.valueOf(rlTagId), info, ProductTagRelDto.Key.INFO, ProductTagRelDto.getInfoDto());
        }

        /**
         * true 表示存在缓存，false 表示不存在缓存
         */
        public static boolean exists(int aid, int unionId) {
            String cacheKey = getCacheKey(aid, unionId);
            return m_cache.exists(cacheKey);
        }

        /**
         * 设置缓存的过期时间为10s
         * @param aid
         * @param unionId
         */
        public static void setExpire(int aid, int unionId) {
            String cacheKey = getCacheKey(aid, unionId);
            m_cache.expire(cacheKey, EXPIRE_SECOND);
        }

    }

    /*** sort 字段的 cache ***/
    public static class SortCache {

        private static final String SORT_CACHE_KEY = "MG_productTagRelSort";

        //添加排序字段的缓存
        public static void set(int aid, int unionPriId, int sort) {
            if(sort < 0) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.set(cacheKey, String.valueOf(sort));
        }

        //删除排序字段的缓存
        public static void del(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            if(!m_cache.exists(cacheKey)) {
                return;
            }
            m_cache.del(cacheKey);
        }

        //获取排序字段的缓存
        public static String get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            if(!m_cache.exists(cacheKey)) {
                return null;
            }
            return m_cache.get(cacheKey);
        }

        //获取排序字段缓存的key
        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(SORT_CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }
    }

    /** 数据状态缓存 **/
    public static class DataStatusCache {

        public static Param get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.getParam(cacheKey, ProductTagRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void add(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.setParam(cacheKey, info, ProductTagRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
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
            m_cache.updateParam(getCacheKey(aid, unionPriId), updater, ProductTagRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void expire(int aid, int unionPriId, int second) {
            m_cache.expire(getCacheKey(aid, unionPriId), second);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(DATA_STATUS_CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }

        private static final String DATA_STATUS_CACHE_KEY = "MG_pdTagRelDS";
    }
}
