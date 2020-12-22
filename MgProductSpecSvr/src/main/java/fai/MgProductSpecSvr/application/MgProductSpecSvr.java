package fai.MgProductSpecSvr.application;

import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.repository.*;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.config.RedisClientConfig;
import fai.comm.cache.redis.pool.JedisPool;
import fai.comm.cache.redis.pool.JedisPoolFactory;
import fai.comm.distributedkit.lock.PosDistributedLock;
import fai.comm.jnetkit.config.ParamKeyMapping;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.util.Log;
import fai.comm.util.Param;

import java.io.IOException;

public class MgProductSpecSvr {


    public static void main(String[] args) throws IOException {
        ServerConfig config = new ServerConfig(args);
        FaiServer server = new FaiServer(config);

        // svr option
        SVR_OPTION = server.getConfig().getConfigObject(SvrOption.class);

        // dao相关
        {
            DaoProxy daoProxy = new DaoProxy(config);
            DaoCtrl.init(daoProxy);
        }
        // codis缓存组件
        {
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
            boolean initCache = CacheCtrl.init(m_cache);
            if(!initCache) {
                Log.logErr("redis cache invalid");
                return;
            }
            // 分布式分段锁
            {
                long lockLease = SVR_OPTION.getLockLease();
                long lockLength = SVR_OPTION.getLockLength();
                Log.logStd("lockLease=%d;", lockLease);
                Log.logStd("lockLength=%d;", lockLength);
                LockUtil.initLock(new PosDistributedLock(m_cache, SVR_OPTION.getLockLength() , LOCK_TYPE, lockLease, m_retryLockTime));
            }
            // 初始化idbuilder
            {
                SpecTempDaoCtrl.initIdBuilder(m_cache);
                SpecTempBizRelDaoCtrl.initIdBuilder(m_cache);
                SpecTempDetailDaoCtrl.initIdBuilder(m_cache);
                SpecStrDaoCtrl.initIdBuilder(m_cache);
                ProductSpecDaoCtrl.initIdBuilder(m_cache);
                ProductSpecSkuDaoCtrl.initIdBuilder(m_cache);
            }
        }

        server.setHandler(new MgProductSpecHandler(server));
        server.start();
    }


    @ParamKeyMapping(path = ".svr")
    public static class SvrOption {
        private int lockLease = 1000;
        private int lockLength = 1000;

        public int getLockLease() {
            return lockLease;
        }

        public void setLockLease(int lockLease) {
            this.lockLease = lockLease;
        }

        public int getLockLength() {
            return lockLength;
        }

        public void setLockLength(int lockLength) {
            this.lockLength = lockLength;
        }
    }
    private static SvrOption SVR_OPTION;
    private static final String LOCK_TYPE = "SPECIFICATION_SVR_LOCK";
    private static long m_retryLockTime = 100L;
}
