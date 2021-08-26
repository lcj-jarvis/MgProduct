package fai.MgProductBasicSvr.domain.repository.cache;

import fai.MgProductBasicSvr.interfaces.dto.ProductBindTagDto;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.misc.Utils;

import java.util.HashSet;

public class ProductBindTagCache extends CacheCtrl {

    public static FaiList<Param> getCacheList(int aid, int unionPriId, HashSet<Integer> pdIds) {
        FaiList<Param> list = null;
        if(Utils.isEmptyList(pdIds)) {
            return null;
        }
        FaiList<String> cacheKeys = new FaiList<>();
        for(Integer pdId : pdIds) {
            cacheKeys.add(getCacheKey(aid, unionPriId, pdId));
        }
        try {
            list = getFaiList(cacheKeys, ProductBindTagDto.getInfoDto(), ProductBindTagDto.Key.INFO);
        } catch (Exception e) {
            Log.logErr(e,"getCacheList error;aid=%d;unionPriId=%d;pdIds=%s;", aid, unionPriId, pdIds);
        }

        return list;
    }

    public static void setExpire(int aid, int unionPriId, FaiList<Integer> pdIds) {
        for (Integer pdId : pdIds) {
            String cacheKey = getCacheKey(aid, unionPriId, pdId);
            m_cache.expire(cacheKey, EXPIRE_SECOND);

        }
    }

    /**
     *  aid + uid + pdId ---》 list
     * @param aid
     * @param unionPriId
     * @param pdId
     * @param list
     */
    public static void addCacheList(int aid, int unionPriId, int pdId, FaiList<Param> list) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        String cacheKey = getCacheKey(aid, unionPriId, pdId);
        m_cache.setFaiList(cacheKey, list, ProductBindTagDto.Key.INFO, ProductBindTagDto.getInfoDto());
    }

    public static void delCache(int aid, int unionPriId, int pdId) {
        String cacheKey = getCacheKey(aid, unionPriId, pdId);
        if (m_cache.exists(cacheKey)) {
            m_cache.del(cacheKey);
        }
    }

    public static void delCacheList(int aid, int unionPriId, FaiList<Integer> pdIds) {
        if(Utils.isEmptyList(pdIds)) {
            return;
        }
        for (Integer pdId : pdIds) {
            String cacheKey = getCacheKey(aid, unionPriId, pdId);
            if (m_cache.exists(cacheKey)) {
                m_cache.del(cacheKey);
            }
        }
    }

    /** 数据状态缓存 **/
    public static class DataStatusCache {
        public static Param get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);
            return m_cache.getParam(cacheKey, ProductBindTagDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
        }

        public static void add(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);
            m_cache.setParam(cacheKey, info, ProductBindTagDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
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
            m_cache.updateParam(getCacheKey(aid, unionPriId), updater, ProductBindTagDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
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
            if(Util.isEmptyList(unionPriIds)) {
                return null;
            }
            String[] keys = new String[unionPriIds.size()];
            int index = 0;
            for(int unionPriId : unionPriIds) {
                keys[index++] = getCacheKey(aid, unionPriId);
            }
            return keys;
        }

        private static final String DATA_STATUS_CACHE_KEY = "MG_pdBindTagDS";
    }

    public static String getCacheKey(int aid, int unionPriId, int rlPdId) {
        return wrapCacheVersion(CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + rlPdId, aid);
    }

    private static final String CACHE_KEY = "MG_productBindTag";
    private static final int EXPIRE_SECOND = 10;
}
