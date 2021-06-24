package fai.MgProductLibSvr.domain.repository.cache;

import fai.MgProductLibSvr.domain.entity.ProductLibRelEntity;
import fai.MgProductLibSvr.interfaces.dto.ProductLibRelDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:22
 */
public class ProductLibRelCache extends CacheCtrl{

    private static final int EXPIRE_SECOND = 10;

    public static class InfoCache {

        private static final String CACHE_KEY = "MG_productLibRel";
        /**
         * 获取缓存的key，格式：MG_productLibRel-aid:HHmmss
         * @return MG_productLibRel-aid:HHmmss
         */
        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }

        /**
         * 添加FaiList<Param>到缓存
         * 缓存的形式：cachkey rlLibId  Param rlLibId Param ....
         */
        public static void addCacheList(int aid, int unionId, FaiList<Param> list) {
            if(list == null || list.isEmpty()) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            m_cache.hmsetFaiList(cacheKey, ProductLibRelEntity.Info.RL_LIB_ID, Var.Type.INT, list, ProductLibRelDto.Key.INFO, ProductLibRelDto.getInfoDto());
        }

        /**
         * 删除缓存的FaiList<Param>
         */
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

        /**
         * 更新缓存的FaiList<Param>
         */
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
                int rlGroupId = info.getInt(ProductLibRelEntity.Info.RL_LIB_ID, 0);
                m_cache.hsetParam(cacheKey, String.valueOf(rlGroupId), updater, ProductLibRelDto.Key.INFO, ProductLibRelDto.getInfoDto());
            }
        }

        /**
         * 获取所有的缓存的FaiList<Param> ---> cacheKey:FaiList<Param>
         */
        public static FaiList<Param> getCacheList(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.hgetAllFaiList(cacheKey, ProductLibRelDto.Key.INFO, ProductLibRelDto.getInfoDto());
        }

        /**
         * 如果keyExists为true，执行hset操作，添加单个缓存到hash中。
         *    缓存的形式：cachekey LibId（byte[]数组的形式） Param（byte[]数组的形式）
         */
        public static void addCache(int aid, int unionId, Param info) {
            if(Str.isEmpty(info)) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionId);
            int rlGroupId = info.getInt(ProductLibRelEntity.Info.RL_LIB_ID, 0);
            m_cache.hsetParam(true, cacheKey, String.valueOf(rlGroupId), info, ProductLibRelDto.Key.INFO, ProductLibRelDto.getInfoDto());
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

        private static final String SORT_CACHE_KEY = "MG_productLibRelSort";

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
            return m_cache.getParam(cacheKey, ProductLibRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void add(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.setParam(cacheKey, info, ProductLibRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
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
            m_cache.updateParam(getCacheKey(aid, unionPriId), updater, ProductLibRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void expire(int aid, int unionPriId, int second) {
            m_cache.expire(getCacheKey(aid, unionPriId), second);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(DATA_STATUS_CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }

        private static final String DATA_STATUS_CACHE_KEY = "MG_pdLibRelDS";
    }
}
