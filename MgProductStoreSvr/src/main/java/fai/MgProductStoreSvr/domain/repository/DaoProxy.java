package fai.MgProductStoreSvr.domain.repository;

import fai.comm.jnetkit.config.FaiListTypeMapping;
import fai.comm.jnetkit.config.ParamKeyMapping;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.util.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * dao代理 range分库支持:[begin, end]、[begin, end]、[begin, end]
 */
public class DaoProxy {
    public DaoProxy(ServerConfig config) {
        DaoProxyOption proxyOption = config.getConfigObject(DaoProxyOption.class);
        String env = proxyOption.getEnv();
        if(!"dev".equalsIgnoreCase(env) && !"pro".equalsIgnoreCase(env)) {
            Log.logErr("unknown env value '%s', only 'pro' or 'dev' is valid", env);
            return;
        }

        int type = "pro".equalsIgnoreCase(env) ? FaiDbUtil.Type.PRO_MASTER : FaiDbUtil.Type.DEV_MASTER;
        FaiList<Param> mainList = proxyOption.getMainList();
        if(mainList == null || mainList.isEmpty()){
            Log.logErr("get mainList err mainList=%s", mainList);
            return;
        }

        DaoPoolWrap daoPoolWrap = new DaoPoolWrap();
        daoPoolWrap.init(type, mainList);
        DaoPoolWrap.DAO_POOL_MAP.put(DaoPoolWrap.DaoPoolType.MAIN, daoPoolWrap);

        FaiList<Param> bakList = proxyOption.getBakList();
        if(bakList != null && !bakList.isEmpty()){
            daoPoolWrap = new DaoPoolWrap();
            daoPoolWrap.init(type, mainList);
            DaoPoolWrap.DAO_POOL_MAP.put(DaoPoolWrap.DaoPoolType.BAK, daoPoolWrap);
        }else{
            DaoPoolWrap.DAO_POOL_MAP.put(DaoPoolWrap.DaoPoolType.BAK, DaoPoolWrap.DAO_POOL_MAP.get(DaoPoolWrap.DaoPoolType.MAIN));
        }

        FaiList<Param> taskList = proxyOption.getTaskList();
        if(taskList != null && !taskList.isEmpty()){
            daoPoolWrap = new DaoPoolWrap();
            daoPoolWrap.init(type, mainList);
            DaoPoolWrap.DAO_POOL_MAP.put(DaoPoolWrap.DaoPoolType.TASK, daoPoolWrap);
        }else{
            DaoPoolWrap.DAO_POOL_MAP.put(DaoPoolWrap.DaoPoolType.TASK, DaoPoolWrap.DAO_POOL_MAP.get(DaoPoolWrap.DaoPoolType.MAIN));
        }
    }
    public Dao getDao(int flow, int aid) {
        return getDao(flow, aid, DaoPoolWrap.DaoPoolType.MAIN);
    }
    public Dao getBakDao(int flow, int aid) {
        return getDao(flow, aid, DaoPoolWrap.DaoPoolType.BAK);
    }
    public Dao getTaskDao(int flow, int aid){
        return getDao(flow, aid, DaoPoolWrap.DaoPoolType.TASK);
    }
    private Dao getDao(int flow, int aid, int daoPoolType) {
        DaoPoolWrap daoPoolWrap = DaoPoolWrap.DAO_POOL_MAP.get(daoPoolType);
        return daoPoolWrap.getDao(flow, aid);
    }


    public void destroy(){
        for (DaoPoolWrap daoPoolWrap : DaoPoolWrap.DAO_POOL_MAP.values()) {
            daoPoolWrap.destroy();
        }
    }

    @ParamKeyMapping(path = ".daoProxy")
    public static class DaoProxyOption {
        private String m_env;
        private FaiList<Param> m_mainList;
        private FaiList<Param> m_bakList;
        private FaiList<Param> m_taskList;

        public void setEnv(String env) {
            m_env = env;
        }
        public String getEnv() {
            return m_env;
        }

        public FaiList<Param> getMainList() {
            return m_mainList;
        }
        public void setMainList(@FaiListTypeMapping(type = Param.class) FaiList<Param> mainList) {
            this.m_mainList = mainList;
        }

        public void setBakList(@FaiListTypeMapping(type = Param.class) FaiList<Param> bakList) {
            m_bakList = bakList;
        }
        public FaiList<Param> getBakList() {
            return m_bakList;
        }

        public void setTaskList(@FaiListTypeMapping(type = Param.class) FaiList<Param> taskList) {
            m_taskList = taskList;
        }
        public FaiList<Param> getTaskList() {
            return m_taskList;
        }

    }


    private static final class DaoPoolWrap{
        public Dao getDao(int flow, int aid) {
            String instance = m_instances[aid % 1000];
            DaoPool daoPool = m_daoPoolMap.get(instance);
            if (daoPool == null) {
                Log.logErr("dao pool get err;flow=%d, db=%s", flow, instance);
                return null;
            }
            Log.logStd("flow=%d, aid=%d, db instance=%s", flow, aid, instance);
            return daoPool.getDao();
        }

        public void destroy() {
            for (DaoPool daoPool : m_daoPoolMap.values()) {
                daoPool.destory();
            }
        }

        private static final class DaoPoolType {
            private static final int MAIN = 1;
            private static final int BAK = 2;
            private static final int TASK = 3;
        }
        private static final Map<Integer, DaoPoolWrap> DAO_POOL_MAP = new HashMap<>();

        private Map<String, DaoPool> m_daoPoolMap;
        private String[] m_instances;
        public DaoPoolWrap() {
            this.m_daoPoolMap = new HashMap<>();
            this.m_instances = new String[1000];
        }
        private void init(int type, FaiList<Param> list) {
            for (Param info : list) {
                String instance = info.getString("instance");
                int maxSize = info.getInt("maxSize");
                DaoPool daoPool = initDaoPool(instance, type, maxSize);
                m_daoPoolMap.put(instance, daoPool);
                int begin = info.getInt("begin");
                int end = info.getInt("end");
                for (int i = begin; i <= end; i++) {
                    m_instances[i] = instance;
                }
            }
        }
        private DaoPool initDaoPool(String instance, int type, int maxSize) {
            Param info = FaiDbUtil.getDbInfo(type, instance);
            if (info == null) {
                Log.logErr("get '%s' dao pool info err which is empty", instance);
                return null;
            }
            String ip = info.getString(FaiDbUtil.DbInfo.IP);
            int port = info.getInt(FaiDbUtil.DbInfo.PORT);
            String db = info.getString(FaiDbUtil.DbInfo.DATABASE);
            String user = info.getString(FaiDbUtil.DbInfo.USER);
            String pwd = info.getString(FaiDbUtil.DbInfo.PASSWORD);
            return new DaoPool(instance, maxSize, ip, port, db, user, pwd);
        }
    }
}
