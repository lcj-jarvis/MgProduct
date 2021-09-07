package fai.MgProductInfSvr.domain.serviceproc;


import fai.MgProductLibSvr.interfaces.cli.MgProductLibCli;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;

/**
 * @author LuChaoJi
 * @date 2021-07-01 11:32
 */
public class ProductLibProc {

    private int m_flow;
    private MgProductLibCli m_cli;

    public ProductLibProc(int flow) {
        this.m_flow = flow;
        m_cli = createCli();
    }

    private MgProductLibCli createCli() {
        MgProductLibCli cli = new MgProductLibCli(m_flow,"MgProductLibCli");
        if(!cli.init()) {
            throw new MgException(Errno.ERROR, "init MgProductLibCli error;flow=%d;", m_flow);
        }
        return cli;
    }

    /**
     * 新增商品库数据，返回商品库id和库业务id
     */
    public void addProductLib(int aid, int tid, int unionPriId, Param info,
                             Ref<Integer> libIdRef, Ref<Integer> rlLibIdRef) {
        int rt = m_cli.addProductLib(aid, tid, unionPriId, info, libIdRef, rlLibIdRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "addProductLib error;flow=%d;aid=%d;tid=%d;uid=%d;", m_flow, aid, tid, unionPriId);
        }
    }

    /**
     * 根据库业务id删除商品库
     */
    public void delLibList(int aid, int unionPriId, FaiList<Integer> rlLibIds) {
        int rt = m_cli.delLibList(aid, unionPriId, rlLibIds);
        if(rt != Errno.OK) {
            throw new MgException(rt, "delLibList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
    }

    /**
     * 修改商品库数据
     */
    public void setLibList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        int rt = m_cli.setLibList(aid, unionPriId, updaterList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "set LibList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
    }

    /**
     * 根据条件查询商品库数据
     */
    public FaiList<Param> getLibList(int aid, int unionPriId, SearchArg searchArg) {
        FaiList<Param> list = new FaiList<>();
        int rt = m_cli.getLibList(aid, unionPriId, searchArg, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getLibList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        return list;
    }

    /**
     * 联合增删改
     */
    public FaiList<Integer> unionSetLibList(int aid, int tid, int unionPriId,
                                            FaiList<Param> addInfoList,
                                            FaiList<ParamUpdater> updaterList,
                                            FaiList<Integer> delRlLibIds) {
        FaiList<Integer> rlLibIdsRef = new FaiList<>();
        int rt = m_cli.unionSetLibList(aid, tid, unionPriId, addInfoList, updaterList, delRlLibIds, rlLibIdsRef);
        if (rt != Errno.OK) {
            throw new MgException(rt, "unionSetLibList error;flow=%d;aid=%d;uid=%d", m_flow, aid, unionPriId);
        }
        return rlLibIdsRef;
    }

    /**
     * 根据条件查询库业务表的数据
     */
    public FaiList<Param> getRelLibList(int aid, int unionPriId, SearchArg searchArg) {
        FaiList<Param> list = new FaiList<>();
        int rt = m_cli.getLibRelFromDb(aid, unionPriId, searchArg, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getRelLibList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        return list;
    }

    /**
     * 克隆数据
     * @param aid
     * @param fromAid
     * @param cloneUnionPriIds
     */
    public void cloneData(int aid, int fromAid, FaiList<Param> cloneUnionPriIds) {
        int rt = m_cli.cloneData(aid, fromAid, cloneUnionPriIds);
        if (rt != Errno.OK) {
            throw new MgException(rt, "cloneData error;flow=%d;aid=%d;uids=%s", m_flow, aid, cloneUnionPriIds);
        }
    }

    /**
     * 增量克隆数据
     * @param aid
     * @param unionPriId
     * @param fromAid
     * @param fromUnionPriId
     */
    public void incrementalClone(int aid, int unionPriId, int fromAid, int fromUnionPriId) {
        int rt = m_cli.incrementalClone(aid, unionPriId, fromAid, fromUnionPriId);
        if (rt != Errno.OK) {
            throw new MgException(rt, "incrementalClone error;flow=%d;aid=%d;uid=%s;fromAid=%d;fromUid=%d;", m_flow, aid, unionPriId, fromAid, fromUnionPriId);
        }
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

}
