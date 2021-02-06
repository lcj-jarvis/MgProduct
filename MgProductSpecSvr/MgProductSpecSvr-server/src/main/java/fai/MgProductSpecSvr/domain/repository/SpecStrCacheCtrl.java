package fai.MgProductSpecSvr.domain.repository;

import fai.MgProductSpecSvr.interfaces.dto.SpecStrDto;
import fai.MgProductSpecSvr.interfaces.entity.SpecStrEntity;
import fai.comm.cache.redis.client.RedisClient;
import fai.comm.util.*;

import java.util.*;

/**
 * 缓存
 */
public class SpecStrCacheCtrl extends CacheCtrl {

    public static boolean setCacheList(int aid, FaiList<Param> infoList) {
        if(infoList == null || infoList.isEmpty()){
            return false;
        }
        Map<String, String> nameIdMap = new HashMap<>(infoList.size()*4/3);
        infoList.forEach(info->{
            Integer id = info.getInt(SpecStrEntity.Info.SC_STR_ID);
            String name = info.getString(SpecStrEntity.Info.NAME);
            if(name == null){
                return;
            }
            nameIdMap.put(name, String.valueOf(id));
        });
        boolean boo =  m_cache.hmsetFaiList(getCacheKey(aid), SpecStrEntity.Info.SC_STR_ID, Var.Type.INT, infoList, SpecStrDto.Key.INFO, SpecStrDto.getDtoDef());
        if(boo){
            boo = m_cache.hmset(getNameIdCacheKey(aid), nameIdMap);
        }
        return boo;
    }

    public static boolean setCacheInfo(int aid, Param info) {
        return setCacheList(aid, new FaiList<>(Arrays.asList(info)));
    }

    public static Param getCacheInfo(int aid, int id) {
        FaiList<Param> cacheList = getCacheList(aid, new FaiList<>(Arrays.asList(id)));
        if(cacheList == null || cacheList.isEmpty()){
            return null;
        }
        return cacheList.get(0);
    }
    public static FaiList<Param> getCacheList(int aid, FaiList<Integer> idList) {
        if(idList == null || idList.isEmpty()){
            return null;
        }
        FaiList<String> idStrList = new FaiList<>(idList.size());
        idList.forEach(id->{
            idStrList.add(String.valueOf(id));
        });

        return getCacheListByIdStrList(aid, idStrList);
    }
    public static FaiList<Param> getCacheListByIdStrList(int aid, FaiList<String> idStrList) {
        if(idStrList == null || idStrList.isEmpty()){
            return null;
        }
        try {
            FaiList<Param> list = m_cache.hmget(getCacheKey(aid), SpecStrDto.Key.INFO, SpecStrDto.getDtoDef(), idStrList);
            if(list == null){
                return null;
            }
            for (int i = list.size() - 1; i >= 0; i--) {
                if(list.get(i) == null){
                    list.remove(i);
                }
            }
            return list;
        } catch (Exception e) {
            Log.logErr(e);
        }
        return null;
    }
    public static FaiList<Param> getCacheListByNames(int aid, FaiList<String> nameList) {
        if(nameList == null || nameList.isEmpty()){
            return null;
        }
        RedisClient redisClient = m_cache.getRedisClient();
        List<String> ids = redisClient.hmget(getNameIdCacheKey(aid), nameList.toArray(new String[nameList.size()]));
        if(ids == null){
            return null;
        }
        for (int i = ids.size() - 1; i >= 0; i--) {
            if(ids.get(i) == null){
                ids.remove(i);
            }
        }
        return getCacheListByIdStrList(aid, new FaiList<>(ids));
    }

    public static Param getCacheInfo(int aid, String name) {
        FaiList<Param> cacheListByNames = getCacheListByNames(aid, new FaiList<>(Arrays.asList(name)));
        if(cacheListByNames == null || cacheListByNames.isEmpty()){
            return null;
        }
        return cacheListByNames.get(0);
    }

    public static boolean delAllCache(int aid) {
        return m_cache.del(getCacheKey(aid), getNameIdCacheKey(aid));
    }

    public static String getNameIdCacheKey(int aid) {
        return CACHE_KEY_PREFIX + ":" + aid + "-name-id";
    }
    public static String getCacheKey(int aid) {
        return CACHE_KEY_PREFIX + ":" + aid;
    }
    private static final String CACHE_KEY_PREFIX = "MG_specStr";


}
