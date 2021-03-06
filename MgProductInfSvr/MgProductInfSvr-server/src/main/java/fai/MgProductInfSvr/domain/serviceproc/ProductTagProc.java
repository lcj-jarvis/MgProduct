package fai.MgProductInfSvr.domain.serviceproc;

import fai.MgProductTagSvr.interfaces.cli.MgProductTagCli;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;

/**
 * @author LuChaoJi
 * @date 2021-07-19 12:06
 */
public class ProductTagProc {

    private int m_flow;
    private MgProductTagCli m_cli;

    public ProductTagProc(int flow) {
        this.m_flow = flow;
        m_cli = createCli();
    }

    private MgProductTagCli createCli() {
        MgProductTagCli cli = new MgProductTagCli(m_flow,"MgProductTagCli");
        if(!cli.init()) {
            throw new MgException(Errno.ERROR, "init MgProductTagCli error;flow=%d;", m_flow);
        }
        return cli;
    }

    public void addProductTag(int aid, int tid, int unionPriId, Param addInfo, Ref<Integer> tagIdRef, Ref<Integer> rlTagIdRef) {
        int rt = m_cli.addProductTag(aid, tid, unionPriId, addInfo, tagIdRef, rlTagIdRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "addProductTag error;flow=%d;aid=%d;tid=%d;uid=%d;", m_flow, aid, tid, unionPriId);
        }
    }

    public void delTagList(int aid, int unionPriId, FaiList<Integer> rlTagIds) {
        int rt = m_cli.delTagList(aid, unionPriId, rlTagIds);
        if(rt != Errno.OK) {
            throw new MgException(rt, "delTagList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
    }

    public void setTagList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        int rt = m_cli.setTagList(aid, unionPriId, updaterList);
        if(rt != Errno.OK) {
            throw new MgException(rt, "set TagList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
    }

    public FaiList<Param> getTagList(int aid, int unionPriId, SearchArg searchArg) {
        FaiList<Param> list = new FaiList<>();
        int rt = m_cli.getTagList(aid, unionPriId, searchArg, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getTagList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        return list;
    }

    public FaiList<Integer> unionSetTagList(int aid, int tid, int unionPriId, 
                                            FaiList<Param> addInfoList, 
                                            FaiList<ParamUpdater> updaterList, 
                                            FaiList<Integer> delRlTagIds) {
        FaiList<Integer> rlTagIdsRef = new FaiList<>();
        int rt = m_cli.unionSetTagList(aid, tid, unionPriId, addInfoList, updaterList, delRlTagIds, rlTagIdsRef);
        if (rt != Errno.OK) {
            throw new MgException(rt, "unionSetTagList error;flow=%d;aid=%d;uid=%d", m_flow, aid, unionPriId);
        }
        return rlTagIdsRef;
    }

    public FaiList<Param> getRlTagList(int aid, int unionPriId, SearchArg searchArg) {
        FaiList<Param> list = new FaiList<>();
        int rt = m_cli.getTagRelFromDb(aid, unionPriId, searchArg, list);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getRlTagList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
        return list;
    }


    /**
     * ????????????
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
     * ??????????????????
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
     * ????????????
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
     * ????????????
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
     * ????????????
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
