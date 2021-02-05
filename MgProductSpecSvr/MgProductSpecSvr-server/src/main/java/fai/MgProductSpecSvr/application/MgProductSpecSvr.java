package fai.MgProductSpecSvr.application;

import fai.MgProductSpecSvr.domain.comm.LockUtil;
import fai.MgProductSpecSvr.domain.repository.*;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.config.RedisClientConfig;
import fai.comm.cache.redis.pool.JedisPool;
import fai.comm.cache.redis.pool.JedisPoolFactory;
import fai.comm.distributedkit.lock.support.FaiLockGenerator;
import fai.comm.jnetkit.config.ParamKeyMapping;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.DaoProxy;

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
                int lockLease = SVR_OPTION.getLockLease();
                int lockLength = SVR_OPTION.getLockLength();
                long retryLockTime = SVR_OPTION.getRetryLockTime();
                Log.logStd("lockLength=%d;", lockLength);
                Log.logStd("lockLease=%d;", lockLease);
                Log.logStd("retryLockTime=%d;", retryLockTime);

                FaiLockGenerator lockGenerator = new FaiLockGenerator(m_cache);
                LockUtil.initLock(lockGenerator, lockLease, retryLockTime);

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
        private long retryLockTime = 200L;

        public long getRetryLockTime() {
            return retryLockTime;
        }

        public void setRetryLockTime(long retryLockTime) {
            this.retryLockTime = retryLockTime;
        }

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
}
