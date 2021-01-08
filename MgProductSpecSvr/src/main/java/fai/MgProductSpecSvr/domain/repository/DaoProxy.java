package fai.MgProductSpecSvr.domain.repository;

import fai.comm.jnetkit.config.FaiListTypeMapping;
import fai.comm.jnetkit.config.ParamKeyMapping;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.util.*;

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
        m_mainInstances = new String[1000];
        m_mainDaoPoolMap = new ConcurrentHashMap<>();
        initInstance(type, mainList, m_mainDaoPoolMap, m_mainInstances);

        FaiList<Param> bakList = proxyOption.getBakList();
        if(bakList == null || bakList.isEmpty()){
            return;
        }
        m_bakInstances = new String[1000];
        m_bakDaoPoolMap = new ConcurrentHashMap<>();
        initInstance(type, bakList, m_bakDaoPoolMap, m_bakInstances);

    }
    public Dao getDao(int flow, int aid) {
        return getDao(flow, aid, m_mainInstances, m_mainDaoPoolMap);
    }
    public Dao getBakDao(int flow, int aid) {
        return getDao(flow, aid, m_bakInstances, m_bakDaoPoolMap);
    }

    private Dao getDao(int flow, int aid, String[] m_bakInstances, Map<String, DaoPool> m_bakDaoPoolMap) {
        String instance = m_bakInstances[aid % 1000];
        DaoPool daoPool = m_bakDaoPoolMap.get(instance);
        if (daoPool == null) {
            Log.logErr("dao pool get err;flow=%d, db=%s", flow, instance);
            return null;
        }
        Log.logStd("flow=%d, aid=%d, db instance=%s", flow, aid, instance);
        return daoPool.getDao();
    }


    private void initInstance(int type, FaiList<Param> list, Map<String, DaoPool> m_bakDaoPoolMap, String[] m_bakInstances) {
        for (Param info : list) {
            String instance = info.getString("instance");
            int maxSize = info.getInt("maxSize");
            DaoPool daoPool = initDaoPool(instance, type, maxSize);
            m_bakDaoPoolMap.put(instance, daoPool);
            int begin = info.getInt("begin");
            int end = info.getInt("end");
            for (int i = begin; i <= end; i++) {
                m_bakInstances[i] = instance;
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

    @ParamKeyMapping(path = ".daoProxy")
    public static class DaoProxyOption {
        public void setBakList(@FaiListTypeMapping(type = Param.class) FaiList<Param> bakList) {
            m_bakList = bakList;
        }
        public FaiList<Param> getBakList() {
            return m_bakList;
        }
        public void setEnv(String env) {
            m_env = env;
        }
        public String getEnv() {
            return m_env;
        }
        private String m_env;

        public FaiList<Param> getMainList() {
            return m_mainList;
        }

        public void setMainList(@FaiListTypeMapping(type = Param.class) FaiList<Param> m_mainList) {
            this.m_mainList = m_mainList;
        }

        private FaiList<Param> m_mainList;
        private FaiList<Param> m_bakList;
    }

    public void destroy(){
        if(m_mainDaoPoolMap != null){
            for (DaoPool daoPool : m_mainDaoPoolMap.values()) {
                daoPool.destory();
            }
        }
        if(m_bakDaoPoolMap != null){
            for (DaoPool daoPool : m_bakDaoPoolMap.values()) {
                daoPool.destory();
            }
        }
    }

    private Map<String, DaoPool> m_mainDaoPoolMap;
    private String[] m_mainInstances;

    private Map<String, DaoPool> m_bakDaoPoolMap;
    private String[] m_bakInstances;
}
