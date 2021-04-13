package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.interfaces.dto.ProductDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

import java.util.List;

public class ProductCacheCtrl extends CacheCtrl {

    /*** 数据缓存 ***/
    public static class InfoCache {
        public static Param getCacheInfo(int aid, int pdId) {
            String cacheKey = getCacheKey(aid);
            return m_cache.hgetParam(cacheKey, pdId, ProductDto.Key.INFO, ProductDto.getInfoDto());
        }

        public static FaiList<Param> getCacheList(int aid, List<String> pdIds) {
            String cacheKey = getCacheKey(aid);
            FaiList<Param> list = null;
            try {
                list = m_cache.hmget(cacheKey, ProductDto.Key.INFO, ProductDto.getInfoDto(), pdIds);
            } catch (Exception e) {
                Log.logErr(e,"getCacheList error;aid=%d;pdIds=%s;", aid, pdIds);
            }
            return list;
        }

        public static void addCache(int aid, Param info) {
            if(Str.isEmpty(info)) {
                return;
            }
            String cacheKey = getCacheKey(aid);
            int pdId = info.getInt(ProductEntity.Info.PD_ID);
            m_cache.hsetParam(true, cacheKey, String.valueOf(pdId), info, ProductDto.Key.INFO, ProductDto.getInfoDto());
        }

        public static void addCacheList(int aid, FaiList<Param> list) {
            if(list == null || list.isEmpty()) {
                return;
            }
            String cacheKey = getCacheKey(aid);
            m_cache.hmsetFaiList(cacheKey, ProductEntity.Info.PD_ID, Var.Type.INT, list, ProductDto.Key.INFO, ProductDto.getInfoDto());
        }

        public static void updateCache(int aid, int pdId, ParamUpdater updater) {
            m_cache.hsetParam(getCacheKey(aid), String.valueOf(pdId), updater, ProductDto.Key.INFO, ProductDto.getInfoDto());
        }

        public static void delCache(int aid) {
            String cacheKey = getCacheKey(aid);
            m_cache.del(cacheKey);
        }

        public static void delCacheList(int aid, FaiList<Integer> pdIds) {
            if(pdIds == null || pdIds.isEmpty()) {
                return;
            }
            String[] pdIdStrs = new String[pdIds.size()];
            for(int i = 0; i < pdIds.size(); i++) {
                pdIdStrs[i] = String.valueOf(pdIds.get(i));
            }
            String cacheKey = getCacheKey(aid);
            m_cache.hdel(cacheKey, pdIdStrs);
        }

        public static void setExpire(int aid) {
            String cacheKey = getCacheKey(aid);
            m_cache.expire(cacheKey, EXPIRE_SECOND);
        }

        public static String getCacheKey(int aid) {
            return CACHE_KEY + "-" + aid;
        }

        private static final String CACHE_KEY = "MG_product";
    }

    /** 数据状态缓存 **/
    public static class DataStatusCache {
        public static Param get(int aid) {
            String cacheKey = getCacheKey(aid);
            return m_cache.getParam(cacheKey, ProductDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void add(int aid, Param info) {
            String cacheKey = getCacheKey(aid);
            m_cache.setParam(cacheKey, info, ProductDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void update(int aid, int addCount) {
            Param info = new Param();
            info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, System.currentTimeMillis());
            ParamUpdater updater = new ParamUpdater(info);
            if(addCount != 0) {
                updater.add(DataStatus.Info.TOTAL_SIZE, ParamUpdater.INC, addCount);
            }
            m_cache.updateParam(getCacheKey(aid), updater, ProductDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void expire(int aid, int second) {
            m_cache.expire(getCacheKey(aid), second);
        }

        public static String getCacheKey(int aid) {
            return DATA_STATUS_CACHE_KEY + "-" + aid;
        }

        private static final String DATA_STATUS_CACHE_KEY = "MG_pdDS";
    }

    private static final int EXPIRE_SECOND = 10;
}
