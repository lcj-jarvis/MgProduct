package fai.MgProductSearchSvr.application;

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

        // 公共配置文件在svr main 的方法做一次初始化
        ConfPool.setFaiConfigGlobalConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey, FaiConfig.EtcType.ENV);

        server.setHandler(new MgProductSearchHandler(server, m_cache));
        server.start();
    }

    public static class SvrConfigGlobalConf{
        public static String svrConfigGlobalConfKey = "mgProductSearchSvr";
        public static String loadFromDbThresholdKey = "loadFromDbThreshold";
    }



    @ParamKeyMapping(path = ".svr")
    public static class SvrOption {
        private int lockLease = 1000;
        private boolean debug = false;
        private String productBasicDbInstance;
        private int dbMaxSize = 10;

        public int getDbMaxSize() {
            return dbMaxSize;
        }

        public void setDbMaxSize(int dbMaxSize) {
            this.dbMaxSize = dbMaxSize;
        }

        public int getLockLease() {
            return lockLease;
        }

        public void setLockLease(int lockLease) {
            this.lockLease = lockLease;
        }

        public boolean getDebug() {
            return debug;
        }

        public void setDebug(boolean debug) {
            this.debug = debug;
        }

        public String getProductBasicDbInstance() {
            return productBasicDbInstance;
        }

        public void setProductBasicDbInstance(String productBasicDbInstance) {
            this.productBasicDbInstance = productBasicDbInstance;
        }
    }
}
