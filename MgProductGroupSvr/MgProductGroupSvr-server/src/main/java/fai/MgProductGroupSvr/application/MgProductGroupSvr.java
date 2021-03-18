package fai.MgProductGroupSvr.application;

import fai.MgProductGroupSvr.domain.common.LockUtil;
import fai.MgProductGroupSvr.domain.repository.*;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.cache.redis.config.RedisClientConfig;
import fai.comm.cache.redis.pool.JedisPool;
import fai.comm.cache.redis.pool.JedisPoolFactory;
import fai.comm.jnetkit.config.ParamKeyMapping;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.util.*;

public class MgProductGroupSvr {
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

        int lockLease = svrOption.getLockLease();
        Log.logStd("lockLease=%d;", lockLease);
        int readLockLength = svrOption.getReadLockLength();
        Log.logStd("readLockLength=%d;", readLockLength);

        init(daoPool, m_cache, lockLease, readLockLength);

        server.setHandler(new MgProductGroupHandler(server));
        server.start();
    }

    /**
     * 获取db信息
     */
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

    /**
     * svr初始化
     */
    public static void init(DaoPool daoPool, RedisCacheManager cache, int lockLease, int readLockLength) {
        LockUtil.init(cache, lockLease, readLockLength);
        CacheCtrl.init(cache);
        ProductGroupDaoCtrl.init(daoPool, cache);
        ProductGroupRelDaoCtrl.init(daoPool, cache);
    }

    @ParamKeyMapping(path = ".svr")
    public static class SvrOption {
        private int lockLease = 1000;
        private int readLockLength = 300;
        private boolean debug = false;
        private String dbInstance;
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

        public int getReadLockLength() {
            return readLockLength;
        }

        public void setReadLockLength(int readLockLength) {
            this.readLockLength = readLockLength;
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
}
