package fai.MgProductGroupSvr.interfaces.cli;

import fai.MgBackupSvr.interfaces.dto.MgBackupDto;
import fai.MgProductGroupSvr.interfaces.cmd.MgProductGroupCmd;
import fai.MgProductGroupSvr.interfaces.dto.ProductGroupRelDto;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.infutil.MgConfPool;
import fai.comm.middleground.app.CloneDef;

public class MgProductGroupCli extends FaiClient {
    public MgProductGroupCli(int flow) {
        super(flow, "MgProductGroupCli");
    }

    /**
     * <p> 初始化
     *
     * @return
     */
    public boolean init() {
        return init("MgProductGroupCli", true);
    }

    public static boolean useProductGroup() {
        Param mgSwitch = MgConfPool.getEnvConf("mgSwitch");
        if (Str.isEmpty(mgSwitch)) {
            return false;
        }
        boolean useProductGroup = mgSwitch.getBoolean("useProductGroup", false);
        return useProductGroup;
    }

    /**
     * 新增商品数据，并添加与当前unionPriId的关联
     */
    public int addProductGroup(int aid, int tid, int unionPriId, Param info, int sysType, Ref<Integer> groupIdRef, Ref<Integer> rlGroupIdRef) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (info == null || info.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "info is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductGroupRelDto.Key.TID, tid);
            sendBody.putInt(ProductGroupRelDto.Key.SYS_TYPE, sysType);
            info.toBuffer(sendBody, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getAllInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.ADD);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.ERROR;
                Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                return m_rt;
            }

            if (groupIdRef != null || rlGroupIdRef != null) {
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlGroupIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductGroupRelDto.Key.RL_GROUP_ID) {
                    Log.logErr(m_rt, "recv rlGroupId codec err");
                    return m_rt;
                }

                m_rt = recvBody.getInt(keyRef, groupIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductGroupRelDto.Key.GROUP_ID) {
                    Log.logErr(m_rt, "recv groupId codec err");
                    return m_rt;
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int getGroupList(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> list) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, ProductGroupRelDto.Key.SEARCH_ARG);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.GET_LIST);
            sendProtocol.addEncodeBody(sendBody);

            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                if (m_rt != Errno.NOT_FOUND) {
                    Log.logErr(m_rt, "recv result err");
                }
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductGroupRelDto.getAllInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductGroupRelDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductGroupRelDto.Key.TOTAL_SIZE) {
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    public int setGroupList(int aid, int tid, int unionPriId, int sysType, FaiList<ParamUpdater> updaterList) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (updaterList == null || updaterList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.TID, tid);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductGroupRelDto.Key.SYS_TYPE, sysType);
            updaterList.toBuffer(sendBody, ProductGroupRelDto.Key.UPDATERLIST, ProductGroupRelDto.getAllInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.BATCH_SET);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int delGroupList(int aid, int unionPriId, FaiList<Integer> idList, int sysType, boolean softDel) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (idList == null || idList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            idList.toBuffer(sendBody, ProductGroupRelDto.Key.RL_GROUP_IDS);
            sendBody.putInt(ProductGroupRelDto.Key.SYS_TYPE, sysType);
            sendBody.putBoolean(ProductGroupRelDto.Key.SOFT_DEL, softDel);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.BATCH_DEL);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int getGroupRelDataStatus(int aid, int unionPriId, Param statusInfo) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            statusInfo.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.GET_REL_DATA_STATUS);
            sendProtocol.addEncodeBody(sendBody);

            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                if (m_rt != Errno.NOT_FOUND) {
                    Log.logErr(m_rt, "recv result err");
                }
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = statusInfo.fromBuffer(recvBody, keyRef, DataStatus.Dto.getDataStatusDto());
            if (m_rt != Errno.OK || keyRef.value != ProductGroupRelDto.Key.DATA_STATUS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    public int searchGroupRelFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> list) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, ProductGroupRelDto.Key.SEARCH_ARG);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.SEARCH_REL);
            sendProtocol.addEncodeBody(sendBody);

            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                if (m_rt != Errno.NOT_FOUND) {
                    Log.logErr(m_rt, "recv result err");
                }
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductGroupRelDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductGroupRelDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductGroupRelDto.Key.TOTAL_SIZE) {
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    public int getAllGroupRel(int aid, int unionPriId, FaiList<Param> list) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.GET_ALL_REL);
            sendProtocol.addEncodeBody(sendBody);

            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                if (m_rt != Errno.NOT_FOUND) {
                    Log.logErr(m_rt, "recv result err");
                }
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductGroupRelDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductGroupRelDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    public int unionSetGroupList(int aid, int tid, int unionPriId, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList, int sysType, boolean softDel, Ref<FaiList<Integer>> rlGroupIdsRef) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductGroupRelDto.Key.TID, tid);
            sendBody.putBoolean(ProductGroupRelDto.Key.SOFT_DEL, softDel);
            if (addList == null) {
                addList = new FaiList<Param>();
            }
            addList.toBuffer(sendBody, ProductGroupRelDto.Key.INFO, ProductGroupRelDto.getAllInfoDto());
            if (updaterList == null) {
                updaterList = new FaiList<ParamUpdater>();
            }
            updaterList.toBuffer(sendBody, ProductGroupRelDto.Key.UPDATERLIST, ProductGroupRelDto.getAllInfoDto());
            if (delList == null) {
                delList = new FaiList<Integer>();
            }
            delList.toBuffer(sendBody, ProductGroupRelDto.Key.RL_GROUP_IDS);
            sendBody.putInt(ProductGroupRelDto.Key.SYS_TYPE, sysType);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.UNION_SET_GROUP_LIST);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            if (!addList.isEmpty()) {
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.ERROR;
                    Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                    return m_rt;
                }

                Ref<Integer> keyRef = new Ref<Integer>();
                FaiList<Integer> ids = new FaiList<Integer>();
                m_rt = ids.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductGroupRelDto.Key.RL_GROUP_IDS) {
                    Log.logErr(m_rt, "recv rlGroupIds codec err");
                    return m_rt;
                }
                rlGroupIdsRef.value = ids;
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 修改分类 （包含 增删改）
     */
    public int setAllGroupList(int aid, int tid, int unionPriId, FaiList<Param> treeDataList, int sysType, int groupLevel, boolean softDel) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductGroupRelDto.Key.TID, tid);
            if (treeDataList == null) {
                treeDataList = new FaiList<Param>();
            }
            treeDataList.toBuffer(sendBody, ProductGroupRelDto.Key.UPDATERLIST, ProductGroupRelDto.getTreeInfoDto());
            sendBody.putInt(ProductGroupRelDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductGroupRelDto.Key.GROUP_LEVEL, groupLevel);
            sendBody.putBoolean(ProductGroupRelDto.Key.SOFT_DEL, softDel);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.SET_ALL_GROUP_LIST);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int cloneData(int aid, int fromAid, FaiList<Param> cloneUnionPriIds) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || Util.isEmptyList(cloneUnionPriIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;fromAid=%d;cloneUids=%s;", aid, fromAid, cloneUnionPriIds);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.FROM_AID, fromAid);
            cloneUnionPriIds.toBuffer(sendBody, ProductGroupRelDto.Key.CLONE_UNION_PRI_IDS, CloneDef.Dto.getInternalDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.CLONE);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int incrementalClone(int aid, int unionPriId, int fromAid, int fromUnionPriId) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;uid=%d;fromAid=%d;fromUid=%d;", aid, unionPriId, fromAid, fromUnionPriId);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductGroupRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductGroupRelDto.Key.FROM_AID, fromAid);
            sendBody.putInt(ProductGroupRelDto.Key.FROM_UNION_PRI_ID, fromUnionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.INCR_CLONE);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int backupData(int aid, FaiList<Integer> unionPriIds, Param backupInfo) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || Util.isEmptyList(unionPriIds) || Str.isEmpty(backupInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;uids=%s;backupInfo=%s;", aid, unionPriIds, backupInfo);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            unionPriIds.toBuffer(sendBody, ProductGroupRelDto.Key.UNION_PRI_ID);
            backupInfo.toBuffer(sendBody, ProductGroupRelDto.Key.BACKUP_INFO, MgBackupDto.getInfoDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.BACKUP);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int restoreBackupData(int aid, FaiList<Integer> unionPriIds, int restoreId, Param backupInfo) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || Util.isEmptyList(unionPriIds) || Str.isEmpty(backupInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;uids=%s;backupInfo=%s;", aid, unionPriIds, backupInfo);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            unionPriIds.toBuffer(sendBody, ProductGroupRelDto.Key.UNION_PRI_ID);
            sendBody.putInt(ProductGroupRelDto.Key.RESTORE_ID, restoreId);
            backupInfo.toBuffer(sendBody, ProductGroupRelDto.Key.BACKUP_INFO, MgBackupDto.getInfoDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.RESTORE);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int delBackupData(int aid, Param backupInfo) {
        if (!useProductGroup()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || Str.isEmpty(backupInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;backupInfo=%s;", aid, backupInfo);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            backupInfo.toBuffer(sendBody, ProductGroupRelDto.Key.BACKUP_INFO, MgBackupDto.getInfoDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductGroupCmd.GroupCmd.DEL_BACKUP);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
}
