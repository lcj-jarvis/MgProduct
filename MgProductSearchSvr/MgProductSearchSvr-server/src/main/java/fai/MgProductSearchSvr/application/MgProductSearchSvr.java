package fai.MgProductSearchSvr.application;

import fai.MgProductSearchSvr.domain.repository.cache.MgProductSearchCache;
import fai.MgProductSearchSvr.domain.repository.cache.MgProductSearchCacheTemp;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.config.RedisClientConfig;
import fai.comm.cache.redis.pool.JedisPool;
import fai.comm.cache.redis.pool.JedisPoolFactory;
import fai.comm.config.FaiConfig;
import fai.comm.jnetkit.config.*;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.util.*;

public class MgProductSearchSvr {

    public static void main(String[] args) throws Exception {
        ServerConfig config = new ServerConfig(args);
        FaiServer server = new FaiServer(config);

        // get svr option
        SvrOption svrOption = server.getConfig().getConfigObject(SvrOption.class);
        boolean debug = svrOption.getDebug();
        Log.logStd("MgProductSearchSvr svrOption debug = %s;", debug);

        // codis缓存组件
        ServerConfig.RedisOption redisOption = config.getRedis();
        if (redisOption == null) {
            Log.logErr("get cache option err");
            return;
        }
        Param redisInfo = redisOption.getRedisOption();
        Log.logStd("redisInfo=%s;", redisInfo);
        RedisClientConfig redisConfig = new RedisClientConfig(redisInfo);
        if (!redisConfig.isValiad()) {
            Log.logErr("redis client config invalid");
            return;
        }
        JedisPool jedisPool = JedisPoolFactory.createJedisPool(redisConfig);
        RedisCacheManager m_cache = new RedisCacheManager(jedisPool, redisConfig.getExpire(), redisConfig.getExpireRandom());

        // 数据缓存组件
        ParamCacheRecycle cacheRecycle = new ParamCacheRecycle(config.getName(),
                svrOption.getCacheHours() * 3600, svrOption.getCacheRecycleIntervalHours() * 3600);

        // 公共配置文件, 在svr main 的方法做一次初始化
        ConfPool.setFaiConfigGlobalConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey, FaiConfig.EtcType.ENV);

        init(m_cache, cacheRecycle);

        server.setHandler(new MgProductSearchHandler(server));
        server.start();
    }

    public static class SvrConfigGlobalConf{
        public static String svrConfigGlobalConfKey = "mgProductSearchSvr";
        public static String loadFromDbThresholdKey = "loadFromDbThreshold";
        public static String useIdFromEsAsInSqlThresholdKey = "useIdFromEsAsInSqlThreshold";
        public static String searchResultCacheConfigKey = "searchResultCacheConfigKey";
    }

    @ParamKeyMapping(path = ".svr")
    public static class SvrOption {
        private boolean debug = false;
        private int cacheHours = 1;
        private int cacheRecycleIntervalHours = 1;

        public boolean getDebug() {
            return debug;
        }
        public void setDebug(boolean debug) {
            this.debug = debug;
        }

        public int getCacheHours() {
            return cacheHours;
        }

        public void setCacheHours(int cacheHours) {
            this.cacheHours = cacheHours;
        }

        public int getCacheRecycleIntervalHours() {
            return cacheRecycleIntervalHours;
        }

        public void setCacheRecycleIntervalHours(int cacheRecycleIntervalHours) {
            this.cacheRecycleIntervalHours = cacheRecycleIntervalHours;
        }
    }

    public static void init(RedisCacheManager cache, ParamCacheRecycle cacheRecycle) {
        MgProductSearchCache.init(cache, cacheRecycle);
        MgProductSearchCacheTemp.init(cache, cacheRecycle);
    }
}
