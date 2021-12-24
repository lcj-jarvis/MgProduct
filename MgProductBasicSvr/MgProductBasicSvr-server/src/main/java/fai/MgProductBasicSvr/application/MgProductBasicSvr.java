package fai.MgProductBasicSvr.application;

import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.common.MgProductCheck;
import fai.MgProductBasicSvr.domain.repository.cache.CacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.*;
import fai.MgProductBasicSvr.domain.repository.dao.bak.*;
import fai.MgProductBasicSvr.domain.repository.dao.saga.*;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.config.RedisClientConfig;
import fai.comm.cache.redis.pool.JedisPool;
import fai.comm.cache.redis.pool.JedisPoolFactory;
import fai.comm.jnetkit.config.ParamKeyMapping;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.util.*;

public class MgProductBasicSvr {
    public static void main(String[] args) throws Exception {
        ServerConfig config = new ServerConfig(args);
        FaiServer server = new FaiServer(config);

        // get svr option
        SvrOption svrOption = server.getConfig().getConfigObject(SvrOption.class);
        String dbInstance = svrOption.getDbInstance();
        Log.logStd("dbInstance: %s;", dbInstance);
        if(Str.isEmpty(dbInstance)) {
            Log.logErr("dbInstance is null error;");
            return;
        }

        int dbType = FaiDbUtil.Type.PRO_MASTER;
        if(svrOption.getDebug()) {
            dbType = FaiDbUtil.Type.DEV_MASTER;
        }

        int maxSize = svrOption.getDbMaxSize();
        DaoPool daoPool = getDaoPool(config.getName(), dbType, maxSize, dbInstance);
        if(daoPool == null) {
            Log.logDbg("daopool null error;");
            return;
        }
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

        LockOption lockOption = server.getConfig().getConfigObject(LockOption.class);
        Log.logStd("lockOption=%s;", lockOption);

        init(daoPool, m_cache, lockOption, svrOption, jedisPool);

        server.setHandler(new MgProductBasicHandler(server, m_cache));
        server.start();
        
    }

    private static DaoPool getDaoPool(String svrName, int type, int maxSize, String instance) {
        // connect database config
        Param dbInfo = FaiDbUtil.getDbInfo(type, instance);
        if (dbInfo == null || dbInfo.isEmpty()) {
            Log.logErr("get daoInfo err");
            return null;
        }
        String ip = dbInfo.getString(FaiDbUtil.DbInfo.IP);
        int port = dbInfo.getInt(FaiDbUtil.DbInfo.PORT);
        String user = dbInfo.getString(FaiDbUtil.DbInfo.USER);
        String pwd = dbInfo.getString(FaiDbUtil.DbInfo.PASSWORD);
        String db = dbInfo.getString(FaiDbUtil.DbInfo.DATABASE);
        DaoPool daoPool = new DaoPool(svrName, maxSize, ip, port, db, user, pwd);

        return daoPool;
    }

    private static void init(DaoPool daoPool, RedisCacheManager cache, LockOption lockOption, SvrOption svrOption, JedisPool jedisPool) {
        // 初始化daopool
        ProductDaoCtrl.init(daoPool, cache);
        ProductSagaDaoCtrl.init(daoPool);
        ProductRelSagaDaoCtrl.init(daoPool);
        ProductBindGroupSagaDaoCtrl.init(daoPool);
        ProductBindTagSagaDaoCtrl.init(daoPool);
        ProductBindPropSagaDaoCtrl.init(daoPool);
        ProductBakDaoCtrl.init(daoPool);
        ProductRelBakDaoCtrl.init(daoPool);
        ProductBindGroupBakDaoCtrl.init(daoPool);
        ProductBindPropBakDaoCtrl.init(daoPool);
        ProductBindTagBakDaoCtrl.init(daoPool);

        ProductRelDaoCtrl.init(daoPool, cache);
        ProductBindPropDaoCtrl.init(daoPool);
        ProductBindGroupDaoCtrl.init(daoPool);
        ProductBindTagDaoCtrl.init(daoPool);
        SagaDaoCtrl.init(daoPool);

        // 缓存初始化
        CacheCtrl.init(cache, jedisPool, svrOption.getCacheSuffix());

        LockUtil.init(cache, lockOption);

        // 设置isDev
        MgProductCheck.setIsDev(svrOption.getDebug());
    }

    @ParamKeyMapping(path = ".svr")
    public static class SvrOption {
        private boolean debug = false;
        private String dbInstance;
        private int dbMaxSize = 10;
        private String cacheSuffix = "";

        public String getCacheSuffix() {
            return cacheSuffix;
        }

        public void setCacheSuffix(String cacheSuffix) {
            this.cacheSuffix = cacheSuffix;
        }

        public int getDbMaxSize() {
            return dbMaxSize;
        }

        public void setDbMaxSize(int dbMaxSize) {
            this.dbMaxSize = dbMaxSize;
        }

        public boolean getDebug() {
            return debug;
        }

        public void setDebug(boolean debug) {
            this.debug = debug;
        }

        public String getDbInstance() {
            return dbInstance;
        }

        public void setDbInstance(String dbInstance) {
            this.dbInstance = dbInstance;
        }
    }

    @ParamKeyMapping(path = ".svr.lock")
    public static class LockOption {
        private int lockLease = 1000;
        private int lockLength = 500;
        private int bakLockLength = 100;
        private int readLockLength = 200;

        public int getLockLength() {
            return lockLength;
        }

        public void setLockLength(int lockLength) {
            this.lockLength = lockLength;
        }

        public int getBakLockLength() {
            return bakLockLength;
        }

        public void setBakLockLength(int bakLockLength) {
            this.bakLockLength = bakLockLength;
        }

        public int getLockLease() {
            return lockLease;
        }

        public void setLockLease(int lockLease) {
            this.lockLease = lockLease;
        }

        public int getReadLockLength() {
            return readLockLength;
        }

        public void setReadLockLength(int readLockLength) {
            this.readLockLength = readLockLength;
        }

        @Override
        public String toString() {
            return "LockOption{" +
                    "lockLease=" + lockLease +
                    ", lockLength=" + lockLength +
                    ", bakLockLength=" + bakLockLength +
                    ", readLockLength=" + readLockLength +
                    '}';
        }
    }
}
