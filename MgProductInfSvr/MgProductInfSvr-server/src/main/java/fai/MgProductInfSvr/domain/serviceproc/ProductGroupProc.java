package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductGroupSvr.interfaces.cli.MgProductGroupCli;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;

public class ProductGroupProc {

    public ProductGroupProc(int flow) {
        this.m_flow = flow;
        m_cli = createCli();
    }

    private MgProductGroupCli createCli() {
        MgProductGroupCli cli = new MgProductGroupCli(m_flow);
        if(!cli.init()) {
            throw new MgException(Errno.ERROR, "init MgProductGroupCli error;flow=%d;", m_flow);
        }
        return cli;
    }

    /**
     * 新增商品分类数据，返回商品分类业务id
     */
    public int addProductGroup(int aid, int tid, int unionPriId, Param info) {
        Ref<Integer> rlGroupIdRef = new Ref<>();
        int rt = m_cli.addProductGroup(aid, tid, unionPriId, info, null, rlGroupIdRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "addProductGroup error;flow=%d;aid=%d;tid=%d;uid=%d;", m_flow, aid, tid, unionPriId);
        }
        return rlGroupIdRef.value;
    }

    /**
     * 获取商品分类数据
     */
    public FaiList<Param> getGroupList(int aid, int unionPriId, SearchArg searchArg) {
        FaiList<Param> list = new FaiList<>();
        int rt = m_cli.getGroupList(aid, unionPriId, searchArg, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getGroupList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        return list;
    }

    /**
     * 修改商品分类数据
     */
    public void setGroupList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        int rt = m_cli.setGroupList(aid, unionPriId, updaterList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "setGroupList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
    }

    /**
     * 删除商品分类数据
     */
    public void delGroupList(int aid, int unionPriId, FaiList<Integer> rlGroupIds, int sysType, boolean softDel) {
        int rt = m_cli.delGroupList(aid, unionPriId, rlGroupIds, sysType, softDel);
        if(rt != Errno.OK) {
            throw new MgException(rt, "delGroupList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
    }

    /**
     * 合并（增、删、改）接口
     */
    public FaiList<Integer> unionSetGroupList(int aid, int tid, int unionPriId, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList, int sysType, boolean softDel) {
        Ref<FaiList<Integer>> rlGroupIdsRef = new Ref<>();
        int rt = m_cli.unionSetGroupList(aid, tid, unionPriId, addList, updaterList, delList, sysType, softDel, rlGroupIdsRef);
        if (rt != Errno.OK) {
            throw new MgException(rt, "unionSetGroupList error;flow=%d;aid=%d;uid=%d", m_flow, aid, unionPriId);
        }
        return rlGroupIdsRef.value;
    }

    /**
     * 修改分类信息 （包含 增删改）
     */
    public FaiList<Integer> setAllGroupList(int aid, int tid, int unionPriId, FaiList<ParamUpdater> updaterList, int sysType, int groupLevel, boolean softDel) {
        Ref<FaiList<Integer>> rlGroupIdsRef = new Ref<>();
        int rt = m_cli.setAllGroupList(aid, tid, unionPriId, updaterList, sysType, groupLevel, softDel, rlGroupIdsRef);
        if (rt != Errno.OK) {
            throw new MgException(rt, "setAllGroupList error;flow=%d;aid=%d;uid=%d", m_flow, aid, unionPriId);
        }
        return rlGroupIdsRef.value;
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
    public void restoreBackup(int aid, FaiList<Integer> unionPriIds, Param backupInfo) {
        int rt = m_cli.restoreBackupData(aid, unionPriIds, backupInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "restoreBackup error;flow=%d;aid=%d;uid=%s;backupInfo=%s;", m_flow, aid, unionPriIds, backupInfo);
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
    private MgProductGroupCli m_cli;
}
