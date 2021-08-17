package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.misc.Utils;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProductRelCacheCtrl extends CacheCtrl {

    /*** 商品业务表数据缓存 ***/
    public static class InfoCache {
        public static boolean exist(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.exists(cacheKey);
        }

        public static Param getCacheInfo(int aid, int unionPriId, int pdId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.hgetParam(cacheKey, pdId, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
        }

        public static FaiList<Param> getCacheList(int aid, int unionPriId, List<String> pdIds) {
            String cacheKey = getCacheKey(aid, unionPriId);
            FaiList<Param> list = null;
            try {
                list = m_cache.hmget(cacheKey, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto(), pdIds);
            } catch (Exception e) {
                Log.logErr(e,"getCacheList error;aid=%d;pdIds=%s;", aid, pdIds);
            }
            return list;
        }

        public static void delCache(int aid, int unionPriId, int pdId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.hdel(cacheKey, String.valueOf(pdId));
        }

        public static void delCacheList(int aid, int unionPriId, FaiList<Integer> pdIds) {
            if(Utils.isEmptyList(pdIds)) {
                return;
            }
            String[] pdIdStrs = new String[pdIds.size()];
            for(int i = 0; i < pdIds.size(); i++) {
                pdIdStrs[i] = String.valueOf(pdIds.get(i));
            }
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.hdel(cacheKey, pdIdStrs);
        }

        public static void addCache(int aid, int unionPriId, Param info) {
            if(Str.isEmpty(info)) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionPriId);
            int pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            m_cache.hsetParam(true, cacheKey, String.valueOf(pdId), info, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
        }

        public static void addCacheList(int aid, int unionPriId, FaiList<Param> list) {
            if(list == null || list.isEmpty()) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.hmsetFaiList(cacheKey, ProductRelEntity.Info.PD_ID, Var.Type.INT, list, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
        }

        public static void updateCache(int aid, int unionPriId, int pdId, ParamUpdater updater) {
            m_cache.hsetParam(getCacheKey(aid, unionPriId), String.valueOf(pdId), updater, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
        }

        public static void setExpire(int aid, int uninoId) {
            String cacheKey = getCacheKey(aid, uninoId);
            m_cache.expire(cacheKey, EXPIRE_SECOND);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId, aid);
        }

        private static final String CACHE_KEY = "MG_productRel"; // 商品业务表数据缓存，aid+unionPriId 做 cache key，pdId做hash key
    }

    /** cache: aid+unionPriId+rlPdId+sysType -> pdId **/
    public static class PdIdCache {

        public static void addCacheList(int aid, int unionPriId, int sysType, FaiList<Param> list) {
            if(Utils.isEmptyList(list)) {
                return;
            }
            String cacheKey = getCacheKey(aid, unionPriId, sysType);
            m_cache.hmsetFaiList(cacheKey, ProductRelEntity.Info.RL_PD_ID, Var.Type.INT, list, ProductRelDto.Key.REDUCED_INFO, ProductRelDto.getReducedInfoDto());
        }

        public static FaiList<Param> getCacheList(int aid, int unionPriId, int sysType, HashSet<Integer> rlPdIds) {
            if(Utils.isEmptyList(rlPdIds)) {
                return null;
            }
            List<String> rlPdIdStrs = rlPdIds.stream().map(String::valueOf).collect(Collectors.toList());
            String cacheKey = getCacheKey(aid, unionPriId, sysType);
            FaiList<Param> list = null;
            try {
                list = m_cache.hmget(cacheKey, ProductRelDto.Key.REDUCED_INFO, ProductRelDto.getReducedInfoDto(), rlPdIdStrs);
            } catch (Exception e) {
                Log.logErr(e,"pdId cache get list error;aid=%d;rlPdIdStrs=%s;", aid, rlPdIdStrs);
            }
            return list;
        }

        public static void delCache(int aid, int unionPriId, int sysType, int rlPdId) {
            String cacheKey = getCacheKey(aid, unionPriId, sysType);
            m_cache.hdel(cacheKey, String.valueOf(rlPdId));
        }

        public static void delCacheList(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds) {
            if(Utils.isEmptyList(rlPdIds)) {
                return;
            }
            String[] rlPdIdStrs = new String[rlPdIds.size()];
            for(int i = 0; i < rlPdIds.size(); i++) {
                rlPdIdStrs[i] = String.valueOf(rlPdIds.get(i));
            }
            String cacheKey = getCacheKey(aid, unionPriId, sysType);
            m_cache.hdel(cacheKey, rlPdIdStrs);
        }

        public static String getCacheKey(int aid, int unionPriId, int sysType) {
            return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + sysType, aid);
        }
        private static final String CACHE_KEY = "MG_pdIdCache";
    }

    /** 数据状态缓存 **/
    public static class DataStatusCache {

        public static Param get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.getParam(cacheKey, ProductRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }
        public static void add(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.setParam(cacheKey, info, ProductRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void update(int aid, int unionPriId, int addCount) {
            Param info = new Param();
            info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, System.currentTimeMillis());
            ParamUpdater updater = new ParamUpdater(info);
            if(addCount != 0) {
                updater.add(DataStatus.Info.TOTAL_SIZE, ParamUpdater.INC, addCount);
            }
            m_cache.updateParam(getCacheKey(aid, unionPriId), updater, ProductRelDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void del(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.del(cacheKey);
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


        private static final String DATA_STATUS_CACHE_KEY = "MG_pdRelDS";
    }

    private static final int EXPIRE_SECOND = 10;
}
