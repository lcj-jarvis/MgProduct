package fai.MgProductLibSvr.domain.repository.cache;


import fai.MgProductLibSvr.domain.entity.ProductLibEntity;
import fai.MgProductLibSvr.interfaces.dto.ProductLibDto;
import fai.comm.util.*;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:21
 */
public class ProductLibCache extends CacheCtrl{

    private static final int EXPIRE_SECOND = 10;
    private static final String CACHE_KEY = "MG_productLib";

    /**
     * 添加FaiList<Param>到缓存中，
     * 缓存的形式：cachkey LibId  Param LibId Param ....
     * @param aid
     * @param list
     */
    public static void addCacheList(int aid, FaiList<Param> list) {
        if(list == null || list.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        m_cache.hmsetFaiList(cacheKey, ProductLibEntity.Info.LIB_ID, Var.Type.INT, list, ProductLibDto.Key.INFO, ProductLibDto.getInfoDto());
    }

    /**
     * 删除所有缓存的FaiList<Param>
     * @param aid
     * @param libIds
     */
    public static void delCacheList(int aid, FaiList<Integer> libIds) {
        if(libIds == null || libIds.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        if(!m_cache.exists(cacheKey)) {
            return;
        }
        String[] libIdStrs = new String[libIds.size()];
        for(int i = 0; i < libIds.size(); i++) {
            libIdStrs[i] = String.valueOf(libIds.get(i));
        }
        m_cache.hdel(cacheKey, libIdStrs);
    }

    /**
     * 更新缓存的FaiList<Param>
     */
    public static void updateCacheList(int aid, FaiList<ParamUpdater> updaterList) {
        if(updaterList == null || updaterList.isEmpty()) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        if(!m_cache.exists(cacheKey)) {
            return;
        }
        for(ParamUpdater updater : updaterList) {
            Param info = updater.getData();
            int libId = info.getInt(ProductLibEntity.Info.LIB_ID, 0);
            m_cache.hsetParam(cacheKey, String.valueOf(libId), updater, ProductLibDto.Key.INFO, ProductLibDto.getInfoDto());
        }
    }

    /**
     * 获取所有的缓存的FaiList<Param> ---> cacheKey:FaiList<Param>
     */
    public static FaiList<Param> getCacheList(int aid) {
        String cacheKey = getCacheKey(aid);
        return m_cache.hgetAllFaiList(cacheKey, ProductLibDto.Key.INFO, ProductLibDto.getInfoDto());
    }


    /**
     * 如果keyExists为true，执行hset操作，添加单个缓存到hash中。
     *    缓存的形式：cacheKey LibId（byte[]数组的形式） Param（byte[]数组的形式）
     */
    public static void addCache(int aid, Param info) {
        if(Str.isEmpty(info)) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        int libId = info.getInt(ProductLibEntity.Info.LIB_ID, 0);
        m_cache.hsetParam(true, cacheKey, String.valueOf(libId), info, ProductLibDto.Key.INFO, ProductLibDto.getInfoDto());
    }

    /**
     * 删除单个缓存FaiList<Param>
     */
    public static void delCache(int aid) {
        m_cache.del(getCacheKey(aid));
    }

    /**
     * 设置缓存的过期时间为10s
     * @param aid
     */
    public static void setExpire(int aid) {
        String cacheKey = getCacheKey(aid);
        m_cache.expire(cacheKey, EXPIRE_SECOND);
    }

    /**
     * 根据aid获取缓存的key,缓存的key的格式为：MG_productLib-aid:HHmmss, HHmmss 为时间
     * @param aid
     * @return MG_productLib-aid:HHmmss, HHmmss 为时间
     */
    public static String getCacheKey(int aid) {
        return wrapCacheVersion(CACHE_KEY + "-" + aid, aid);
    }

}
