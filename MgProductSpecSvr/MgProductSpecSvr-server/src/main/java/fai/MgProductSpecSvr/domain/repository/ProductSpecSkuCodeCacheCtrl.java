package fai.MgProductSpecSvr.domain.repository;


import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuCodeDao;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.client.RedisClient;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

import java.util.*;
import java.util.stream.Collectors;

public class ProductSpecSkuCodeCacheCtrl extends CacheCtrl {

    /**
     * 设置数据缓存
     */
    public static boolean setInfoCache(int aid, Map<Long, FaiList<Param>> skuIdInfoListMap){
        if(skuIdInfoListMap == null || skuIdInfoListMap.isEmpty()){
            return false;
        }
        String key = getInfoCacheKey(aid);
        byte[] keyBytes = key.getBytes();
        Map<byte[], byte[]> hash = new HashMap<>(skuIdInfoListMap.size()*4/3+1);
        for (Map.Entry<Long, FaiList<Param>> skuIdInfoListEntry : skuIdInfoListMap.entrySet()) {
            Long skuId = skuIdInfoListEntry.getKey();
            FaiList<Param> infoList = skuIdInfoListEntry.getValue();
            FaiBuffer faiBuffer = new FaiBuffer();
            int rt = infoList.toBuffer(faiBuffer, ProductSpecSkuCodeDao.Key.INFO_LIST, ProductSpecSkuCodeDao.getInfoDto());
            if(rt != Errno.OK){
                Log.logErr("infoList to buffer err");
                return false;
            }
            hash.put(String.valueOf(skuId).getBytes(), Arrays.copyOfRange(faiBuffer.buffer().array(), 0, faiBuffer.buffer().limit())); //存储有用部分的数据
        }

        RedisClient redisClient = m_cache.getRedisClient();
        String res = redisClient.hmset(keyBytes, hash);
        boolean boo = RedisCacheManager.REDIS_RSP_OK.equals(res);
        if(boo){
            m_cache.expire(key, m_cache.getExpireSecond(), m_cache.getExpireRandomSecond());
        }
        Log.logDbg("whalelog  aid=%s;boo=%s;", aid, boo);
        return boo;
    }

    /**
     * 获取数据缓存
     */
    public static Map<Long, FaiList<Param>> getInfoCache(int aid, HashSet<Long> skuIdSet) {
        String key = getInfoCacheKey(aid);
        byte[] keyBytes = key.getBytes();
        List<byte[]> skuIdByteList = skuIdSet.stream().map(skuId->String.valueOf(skuId).getBytes()).collect(Collectors.toList());
        RedisClient redisClient = m_cache.getRedisClient();
        List<byte[]> resultList = redisClient.hmget(keyBytes, skuIdByteList.toArray(new byte[][]{}));
        Map<Long, FaiList<Param>> skuIdInfoListMap = new HashMap<>();
        for (int i = 0; i < resultList.size(); i++) {
            byte[] bytes = resultList.get(i);
            if(bytes == null){
                continue;
            }
            Long skuId = Long.valueOf(new String(skuIdByteList.get(i)));
            FaiBuffer faiBuffer = new FaiBuffer(bytes);
            FaiList<Param> infoList = new FaiList<>();
            infoList.fromBuffer(faiBuffer, null, ProductSpecSkuCodeDao.getInfoDto());
            skuIdInfoListMap.put(skuId, infoList);
        }
        return skuIdInfoListMap;
    }

    /**
     * 删除数据缓存
     */
    public static boolean delInfoCache(int aid, Set<Long> skuIdSet) {
        if(skuIdSet == null || skuIdSet.isEmpty()){
            return true;
        }
        Set<String> skuIdStrSet = skuIdSet.stream().map(String::valueOf).collect(Collectors.toSet());
        String infoCacheKey = getInfoCacheKey(aid);
        return m_cache.hdel(infoCacheKey, skuIdStrSet.toArray(new String[]{}));
    }

    public static boolean delAllCache(int aid){
        return m_cache.del(getInfoCacheKey(aid));
    }

    public static String getInfoCacheKey(int aid){
        return CACHE_KEY_PREFIX+":"+aid;
    }
    /**
     * 数据状态缓存
     **/
    public static class DataStatusCache {
        public static class DataFlag {
            public static final int TOTAL_SIZE = 0x1;
            public static final int MANAGE_LAST_UPDATE_TIME = 0x2;
        }

        public static Param get(int aid, int unionPriId) {
            String cacheKey = getCacheKey(aid, unionPriId);

            RedisClient redisClient = m_cache.getRedisClient();
            String[] fields = {DataStatus.Info.TOTAL_SIZE, DataStatus.Info.MANAGE_LAST_UPDATE_TIME, DataStatus.Info.VISITOR_LAST_UPDATE_TIME};
            List<String> values = redisClient.hmget(cacheKey, fields);
            Param info = new Param();
            if (values == null || values.size() != fields.length) {
                Log.logErr("hmget err;aid=%s;unionPriId=%s;", aid, unionPriId);
                return info;
            }

            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                ParamDef.Record record = DataStatus.Dto.getDataStatusDto().get(field);
                Object value = values.get(i);
                if (value != null) {
                    switch (record.valueType) {
                        case Var.Type.INT:
                            value = Parser.parseInt(value.toString(), 0);
                            break;
                        case Var.Type.LONG:
                            value = Parser.parseLong(value.toString(), 0L);
                            break;
                        default:
                            Log.logErr("unknown type;aid=%s;unionPriId=%s;field=%s;value=%s;", aid, unionPriId, field, value);
                    }
                }
                info.setObject(field, value);
            }
            // 更新缓存时间
            m_cache.expire(cacheKey, m_cache.getExpireSecond(), m_cache.getExpireRandomSecond());
            Log.logDbg("whalelog  info=%s;", info);
            return info;
        }

        public static boolean set(int aid, int unionPriId, Param info) {
            String cacheKey = getCacheKey(aid, unionPriId);

            HashMap<String, String> map = new HashMap<>(info.keySet().size());
            for (String key : info.keySet()) {
                map.put(key, String.valueOf(info.getObject(key)));
            }
            Log.logDbg("whalelog  map=%s;", map);
            return m_cache.hmset(cacheKey, map);
        }

        public static boolean clearDirty(int aid, int unionPriId, int dataFlag) {
            String cacheKey = getCacheKey(aid, unionPriId);
            HashMap<String, String> map = new HashMap<>(2);
            long now = System.currentTimeMillis();
            if (Misc.checkBit(dataFlag, DataFlag.TOTAL_SIZE)) {
                map.put(DataStatus.Info.TOTAL_SIZE, "-1");
            }
            if (Misc.checkBit(dataFlag, DataFlag.MANAGE_LAST_UPDATE_TIME)) {
                map.put(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, String.valueOf(now));
            }
            Log.logDbg("whalelog  map=%s;", map);
            return m_cache.hmset(cacheKey, map);
        }

        public static void expire(int aid, int unionPriId, int second) {
            m_cache.expire(getCacheKey(aid, unionPriId), second);
        }

        public static String getCacheKey(int aid, int unionPriId) {
            return wrapCacheVersion(CACHE_KEY_PREFIX + "-ds:" + aid + "-" + unionPriId, aid);
        }
    }

    private static final String CACHE_KEY_PREFIX = "MG_productSpecSkuCode";
}
