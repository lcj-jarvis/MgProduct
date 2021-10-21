package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductPropSvr.interfaces.cli.MgProductPropCli;
import fai.MgProductPropSvr.interfaces.entity.ProductPropRelEntity;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;

/**
 * 商品参数服务
 */
public class ProductPropProc {
    public ProductPropProc(int flow) {
        this.m_flow = flow;
        m_cli = new MgProductPropCli(flow);
        if(!m_cli.init()) {
            m_cli = null;
        }
    }

    public int getPropList(int aid, int tid, int unionPriId, int libId, SearchArg searchArg, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPropList(aid, tid, unionPriId, libId, searchArg, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPropList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int addPropList(int aid, int tid, int unionPriId, int libId, FaiList<Param> list, Ref<FaiList<Integer>> rlPropIdsRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.addPropList(aid, tid, unionPriId, libId, list, rlPropIdsRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPropList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int setPropInfo(int aid, int tid, int unionPriId, int libId, int rlPropId, ParamUpdater propUpdater) {
        Param info = propUpdater.getData();
        info.setInt(ProductPropRelEntity.Info.RL_PROP_ID, rlPropId);
        FaiList<ParamUpdater> updaterList = new FaiList<ParamUpdater>();
        updaterList.add(propUpdater);
        return setPropList(aid, tid, unionPriId, libId, updaterList);
    }

    public int setPropList(int aid, int tid, int unionPriId, int libId, FaiList<ParamUpdater> updaterList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPropList(aid, tid, unionPriId, libId, updaterList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPropList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int delPropList(int aid, int tid, int unionPriId, int libId, FaiList<Integer> idList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.delPropList(aid, tid, unionPriId, libId, idList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPropList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int clearRelData(int aid, int unionPriId) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.clearRelData(aid, unionPriId);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPropList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int clearAcct(int aid, FaiList<Integer> unionPriIds) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        rt = m_cli.clearAcct(aid, unionPriIds);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPropList error;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
            return rt;
        }
        return rt;
    }

    public int unionSetPropList(int aid, int tid, int unionPriId, int libId, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList, Ref<FaiList<Integer>> idsRef) {
        int rt;
        if (m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.unionSetPropList(aid, tid, unionPriId, libId, addList, updaterList, delList, idsRef);
        if (rt != Errno.OK) {
            Log.logErr(rt, "unionSetPropList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int getPropValList(int aid, int tid, int unionPriId, int libId, FaiList<Integer> rlPropIds, FaiList<Param> list) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.getPropValList(aid, tid, unionPriId, libId, rlPropIds, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPropList error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int addPropInfoWithVal(int aid, int tid, int unionPriId, int libId, Param propInfo, FaiList<Param> proValList, Ref<Integer> rlPropIdRef) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.addPropInfoWithVal(aid, tid, unionPriId, libId, propInfo, proValList, rlPropIdRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "addPropInfoWithVal error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        return rt;
    }

    public int setPropValList(int aid, int tid, int unionPriId, int libId, int rlPropId, FaiList<Param> addList, FaiList<ParamUpdater> setList, FaiList<Integer> delList) {
        int rt = Errno.ERROR;
        if(m_cli == null) {
            rt = Errno.ERROR;
            Log.logErr(rt, "get ProductPropCli error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
            return rt;
        }
        rt = m_cli.setPropValList(aid, tid, unionPriId, libId, rlPropId, addList, setList, delList);
        if(rt != Errno.OK) {
            Log.logErr(rt, "setPropValList error;flow=%d;aid=%d;unionPriId=%d;rlPropId=%d;", m_flow, aid, unionPriId, rlPropId);
            return rt;
        }
        return rt;
    }

    /**
     * 备份数据
     * @param aid
     * @param unionPriIds
     * @param backupInfo
     */
    public void backupData(int aid, FaiList<Integer> unionPriIds, Param backupInfo) {
        int rt = m_cli.backupData(aid, unionPriIds, backupInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "backupData error;flow=%d;aid=%d;uid=%s;backupInfo=%s;", m_flow, aid, unionPriIds, backupInfo);
        }
    }

    /**
     * 还原备份
     * @param aid
     * @param unionPriIds
     * @param backupInfo
     */
    public void restoreBackup(int aid, FaiList<Integer> unionPriIds, int restoreId, Param backupInfo) {
        int rt = m_cli.restoreBackupData(aid, unionPriIds, restoreId, backupInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "restoreBackup error;flow=%d;aid=%d;uid=%s;restoreId=%s;backupInfo=%s;", m_flow, aid, unionPriIds, restoreId, backupInfo);
        }
    }

    /**
     * 删除备份
     * @param aid
     * @param backupInfo
     */
    public void delBackup(int aid, Param backupInfo) {
        int rt = m_cli.delBackupData(aid, backupInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "restoreBackup error;flow=%d;aid=%d;backupInfo=%s;", m_flow, aid, backupInfo);
        }
    }

    private int m_flow;
    private MgProductPropCli m_cli;
}
