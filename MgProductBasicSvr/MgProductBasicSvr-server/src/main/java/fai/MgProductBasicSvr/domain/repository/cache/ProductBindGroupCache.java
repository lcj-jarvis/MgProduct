package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindGroupDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

import java.util.List;

public class ProductBindGroupCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, List<String> rlPdIds) {
        String cacheKey = getCacheKey(aid, unionPriId);
        FaiList<Param> list = null;
        try {
            m_cache.hmget(cacheKey, ProductBindGroupDto.Key.INFO, ProductBindGroupDto.getInfoDto(), rlPdIds);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
        }
        return list;
    }

    public static void addCacheList(int aid, int unionPriId, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId);
        m_cache.hmsetFaiList(cacheKey, ProductBindGroupEntity.Info.RL_GROUP_ID, Var.Type.INT, list, ProductBindGroupDto.Key.INFO, ProductBindGroupDto.getInfoDto());
    }

    public static void delCache(int aid, int unionPriId) {
        String cacheKey = getCacheKey(aid, unionPriId);
        if(m_cache.exists(cacheKey)) {
            m_cache.del(cacheKey);
        }
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

        public static void expire(int aid, int unionPriId, int second) {
            m_cache.expire(getCacheKey(aid, unionPriId), second);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return DATA_STATUS_CACHE_KEY + "-" + aid + "-" + unionPriId;
        }

        private static final String DATA_STATUS_CACHE_KEY = "MG_pdBindGroupDS";
    }

    public static String getCacheKey(int aid, int unionPriId) {
        return CACHE_KEY + "-" + aid + "-" + unionPriId;
    }

    private static final String CACHE_KEY = "MG_productBindGroup";
}