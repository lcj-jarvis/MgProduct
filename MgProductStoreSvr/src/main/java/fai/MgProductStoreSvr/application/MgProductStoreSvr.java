package fai.MgProductStoreSvr.application;

import fai.MgProductStoreSvr.application.task.BizSalesReportTask;
import fai.MgProductStoreSvr.application.task.HoldingStoreMakeUpTask;
import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.repository.*;
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

public class MgProductStoreSvr {

    public static void main(String[] args) throws IOException {
        ServerConfig config = new ServerConfig(args);
        FaiServer server = new FaiServer(config);

        // svr option
        SVR_OPTION = config.getConfigObject(SvrOption.class);

        DaoProxy daoProxy = new DaoProxy(config);
        // dao相关
        {
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
                int lockLength = SVR_OPTION.getLockLength();
                long retryLockTime = SVR_OPTION.getRetryLockTime();
                Log.logStd("lockLength=%d;", lockLength);
                Log.logStd("lockLease=%d;", lockLease);
                Log.logStd("retryLockTime=%d;", retryLockTime);
                LockUtil.initLock(new PosDistributedLock(m_cache, lockLength , LOCK_TYPE, lockLease, retryLockTime));
            }
            // 初始化idbuilder
            {
                InOutStoreRecordDaoCtrl.initIdBuilder(m_cache);
            }
        }
        // 异步任务
        {
            /*BizSalesReportTaskOption bizSalesReportTaskOption = config.getConfigObject(BizSalesReportTaskOption.class);
            Log.logStd("%s", bizSalesReportTaskOption);
            if(bizSalesReportTaskOption.isOpen()){
                Thread bizSalesReportTask = new Thread(new BizSalesReportTask(daoProxy, bizSalesReportTaskOption), "BizSalesReportTask");
                bizSalesReportTask.start();
                server.addCoreThread(bizSalesReportTask);
            }*/
            HoldingStoreMakeUpTaskOption holdingStoreMakeUpTaskOption = config.getConfigObject(HoldingStoreMakeUpTaskOption.class);
            Log.logStd("%s", holdingStoreMakeUpTaskOption);
            if(holdingStoreMakeUpTaskOption.isOpen()){
                Thread holdingStoreMakeUpTask = new Thread(new HoldingStoreMakeUpTask(daoProxy, holdingStoreMakeUpTaskOption), "HoldingStoreMakeUpTask");
                holdingStoreMakeUpTask.start();
                server.addCoreThread(holdingStoreMakeUpTask);
            }
        }
        server.setHandler(new MgProductStoreHandler(server));
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
    @ParamKeyMapping(path = ".svr.bizSalesReportTask")
    public static class BizSalesReportTaskOption {
        private boolean open = false;
        private int startSleepMinutes = 5; // 延迟启动
        private int batchSize = 10;
        private int errSleepSeconds = 10;
        private int emptyTaskSleepSeconds = 1;

        public boolean isOpen() {
            return open;
        }

        public void setOpen(boolean open) {
            this.open = open;
        }

        public int getStartSleepMinutes() {
            return startSleepMinutes;
        }

        public void setStartSleepMinutes(int startSleepMinutes) {
            this.startSleepMinutes = startSleepMinutes;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getErrSleepSeconds() {
            return errSleepSeconds;
        }

        public void setErrSleepSeconds(int errSleepSeconds) {
            this.errSleepSeconds = errSleepSeconds;
        }

        public int getEmptyTaskSleepSeconds() {
            return emptyTaskSleepSeconds;
        }

        public void setEmptyTaskSleepSeconds(int emptyTaskSleepSeconds) {
            this.emptyTaskSleepSeconds = emptyTaskSleepSeconds;
        }

        @Override
        public String toString() {
            return "BizSalesReportTaskOption{" +
                    "open=" + open +
                    ", startSleepMinutes=" + startSleepMinutes +
                    ", batchSize=" + batchSize +
                    ", errSleepSeconds=" + errSleepSeconds +
                    ", emptyTaskSleepSeconds=" + emptyTaskSleepSeconds +
                    '}';
        }
    }
    @ParamKeyMapping(path = ".svr.holdingStoreMakeUpTask")
    public static class HoldingStoreMakeUpTaskOption {
        private boolean open = false;
        private int startSleepMinutes = 5; // 延迟启动
        private int intervalSeconds = 60;

        public boolean isOpen() {
            return open;
        }

        public void setOpen(boolean open) {
            this.open = open;
        }

        public int getStartSleepMinutes() {
            return startSleepMinutes;
        }

        public void setStartSleepMinutes(int startSleepMinutes) {
            this.startSleepMinutes = startSleepMinutes;
        }

        public int getIntervalSeconds() {
            return intervalSeconds;
        }

        public void setIntervalSeconds(int intervalSeconds) {
            this.intervalSeconds = intervalSeconds;
        }

        @Override
        public String toString() {
            return "HoldingStoreMakeUpTaskOption{" +
                    "open=" + open +
                    ", startSleepMinutes=" + startSleepMinutes +
                    ", intervalSeconds=" + intervalSeconds +
                    '}';
        }
    }
    private static SvrOption SVR_OPTION;
    private static final String LOCK_TYPE = "MG_PRODUCT_STORE_SVR_LOCK";
}
