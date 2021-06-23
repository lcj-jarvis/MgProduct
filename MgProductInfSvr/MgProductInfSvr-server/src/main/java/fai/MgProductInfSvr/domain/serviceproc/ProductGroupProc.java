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
    public void delGroupList(int aid, int unionPriId, FaiList<Integer> rlGroupIds) {
        int rt = m_cli.delGroupList(aid, unionPriId, rlGroupIds);
        if(rt != Errno.OK) {
            throw new MgException(rt, "delGroupList error;flow=%d;aid=%d;uid=%d;", m_flow, aid, unionPriId);
        }
    }

    /**
     * 合并（增、删、改）接口
     */
    public int unionSetGroupList(int aid, int tid, int unionPriId, Param addInfo, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList) {
        Ref<Integer> rlGroupIdRef = new Ref<>();
        int rt = m_cli.unionSetGroupList(aid, tid, unionPriId, addInfo, updaterList, delList, rlGroupIdRef);
        if (rt != Errno.OK) {
            throw new MgException(rt, "unionSetGroupList error;flow=%d;aid=%d;uid=%d", m_flow, aid, unionPriId);
        }
        return rlGroupIdRef.value;
    }

    private int m_flow;
    private MgProductGroupCli m_cli;
}
