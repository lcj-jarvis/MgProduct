package fai.MgProductTagSvr.application.domain.repository.cache;

import fai.MgProductTagSvr.application.domain.entity.ProductTagEntity;
import fai.MgProductTagSvr.interfaces.dto.ProductTagDto;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;

/**
 * @author LuChaoJi
 * @date 2021-07-12 14:00
 */
public class ProductTagCache extends CacheCtrl{
    
    private static final int EXPIRE_SECOND = 10;
    private static final String CACHE_KEY = "MG_productTag";

    /**
     * 添加FaiList<Param>到缓存中，
     * 缓存的形式：cacheKey TagId Param TagId Param ....
     * @param aid
     * @param list
     */
    public static void addCacheList(int aid, FaiList<Param> list) {
        if(Util.isEmptyList(list)) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        m_cache.hmsetFaiList(cacheKey, ProductTagEntity.Info.TAG_ID, Var.Type.INT, list, ProductTagDto.Key.INFO, ProductTagDto.getInfoDto());
    }
    
    /**
     * 删除所有缓存的FaiList<Param>
     * @param aid
     * @param tagIds
     */
    public static void delCacheList(int aid, FaiList<Integer> tagIds) {
        if(Util.isEmptyList(tagIds)) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        if(!m_cache.exists(cacheKey)) {
            return;
        }
        String[] tagIdStrs = new String[tagIds.size()];
        for(int i = 0; i < tagIds.size(); i++) {
            tagIdStrs[i] = String.valueOf(tagIds.get(i));
        }
        m_cache.hdel(cacheKey, tagIdStrs);
    }

    /**
     * 更新缓存的FaiList<Param>
     */
    public static void updateCacheList(int aid, FaiList<ParamUpdater> updaterList) {
        if(Util.isEmptyList(updaterList)) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        if(!m_cache.exists(cacheKey)) {
            return;
        }
        for(ParamUpdater updater : updaterList) {
            Param info = updater.getData();
            int tagId = info.getInt(ProductTagEntity.Info.TAG_ID, 0);
            m_cache.hsetParam(cacheKey, String.valueOf(tagId), updater, ProductTagDto.Key.INFO, ProductTagDto.getInfoDto());
        }
    }

    /**
     * 获取所有的缓存的FaiList<Param> ---> cacheKey:FaiList<Param>
     */
    public static FaiList<Param> getCacheList(int aid) {
        String cacheKey = getCacheKey(aid);
        return m_cache.hgetAllFaiList(cacheKey, ProductTagDto.Key.INFO, ProductTagDto.getInfoDto());
    }


    /**
     * 如果keyExists为true，执行hset操作，添加单个缓存到hash中。
     *    缓存的形式：cacheKey TagId（byte[]数组的形式） Param（byte[]数组的形式）
     */
    public static void addCache(int aid, Param info) {
        if(Str.isEmpty(info)) {
            return;
        }
        String cacheKey = getCacheKey(aid);
        int tagId = info.getInt(ProductTagEntity.Info.TAG_ID, 0);
        m_cache.hsetParam(true, cacheKey, String.valueOf(tagId), info, ProductTagDto.Key.INFO, ProductTagDto.getInfoDto());
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
     * 根据aid获取缓存的key,缓存的key的格式为：MG_productTag-aid:HHmmss, HHmmss 为时间
     * @param aid
     * @return MG_productLib-aid:HHmmss, HHmmss 为时间
     */
    public static String getCacheKey(int aid) {
        return wrapCacheVersion(CACHE_KEY + "-" + aid, aid);
    }
}
