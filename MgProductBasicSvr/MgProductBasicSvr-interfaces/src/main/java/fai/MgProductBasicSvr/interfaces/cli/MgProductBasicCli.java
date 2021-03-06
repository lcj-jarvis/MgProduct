package fai.MgProductBasicSvr.interfaces.cli;

import fai.MgBackupSvr.interfaces.dto.MgBackupDto;
import fai.MgProductBasicSvr.interfaces.cmd.MgProductBasicCmd;
import fai.MgProductBasicSvr.interfaces.dto.*;
import fai.comm.middleground.app.CloneDef;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;

public class MgProductBasicCli extends FaiClient {
    public MgProductBasicCli(int flow) {
        super(flow, "MgProductBasicCli");
    }

    /**
     * <p> 初始化
     * @return
     */
    public boolean init() {
        return init("MgProductBasicCli", true);
    }

    public int backupData(int aid, FaiList<Integer> unionPriIds, Param backupInfo) {
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
            unionPriIds.toBuffer(sendBody, ProductRelDto.Key.UNION_PRI_ID);
            backupInfo.toBuffer(sendBody, ProductRelDto.Key.BACKUP_INFO, MgBackupDto.getInfoDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.Cmd.BACKUP);
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
            unionPriIds.toBuffer(sendBody, ProductRelDto.Key.UNION_PRI_ID);
            sendBody.putInt(ProductRelDto.Key.RESTORE_ID, restoreId);
            backupInfo.toBuffer(sendBody, ProductRelDto.Key.BACKUP_INFO, MgBackupDto.getInfoDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.Cmd.RESTORE);
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
            backupInfo.toBuffer(sendBody, ProductRelDto.Key.BACKUP_INFO, MgBackupDto.getInfoDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.Cmd.DEL_BACKUP);
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

    public int cloneData(int aid, int tid, int siteId, int fromAid, FaiList<Param> cloneUnionPriIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || cloneUnionPriIds == null || cloneUnionPriIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;fromAid=%d;cloneUids=%s;", aid, fromAid, cloneUnionPriIds);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductRelDto.Key.FROM_AID, fromAid);
            cloneUnionPriIds.toBuffer(sendBody, ProductRelDto.Key.CLONE_UNION_PRI_IDS, CloneDef.Dto.getInternalDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.Cmd.CLONE);
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

    public int incrementalClone(int aid, int tid, int siteId, int unionPriId, int fromAid, int fromUnionPriId) {
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
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.FROM_AID, fromAid);
            sendBody.putInt(ProductRelDto.Key.FROM_UNION_PRI_ID, fromUnionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.Cmd.INCR_CLONE);
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

    public int getPdBindProp(int aid, int tid, int unionPriId, int sysType, int rlPdId, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindPropDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductBindPropDto.Key.TID, tid);
            sendBody.putInt(ProductBindPropDto.Key.RL_PD_ID, rlPdId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindPropCmd.GET_LIST);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBindPropDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindPropDto.Key.INFO_LIST) {
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

    public int setPdBindProp(int aid, int tid, int unionPriId, int sysType, int rlPdId, FaiList<Param> addList, FaiList<Param> delList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(addList == null) {
                addList = new FaiList<Param>();
            }
            if(delList == null) {
                delList = new FaiList<Param>();
            }
            if(addList.isEmpty() && delList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList and delList all empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindPropDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductBindPropDto.Key.TID, tid);
            sendBody.putInt(ProductBindPropDto.Key.RL_PD_ID, rlPdId);
            addList.toBuffer(sendBody, ProductBindPropDto.Key.PROP_BIND, ProductBindPropDto.getInfoDto());
            delList.toBuffer(sendBody, ProductBindPropDto.Key.DEL_PROP_BIND, ProductBindPropDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BindPropCmd.BATCH_SET);
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

    public int delPdBindProp(int aid, int unionPriId, int sysType, FaiList<Integer> rlPropIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(rlPropIds == null || rlPropIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;del ids is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindPropDto.Key.SYS_TYPE, sysType);
            rlPropIds.toBuffer(sendBody, ProductBindPropDto.Key.RL_PROP_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BindPropCmd.DEL_BY_PROP_IDS);
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

    public int delPdBindProp(int aid, int unionPriId, int sysType, int rlPropId, FaiList<Integer> delPropValIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(delPropValIds == null || delPropValIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;del ids is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindPropDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductBindPropDto.Key.RL_PROP_ID, rlPropId);
            delPropValIds.toBuffer(sendBody, ProductBindPropDto.Key.PROP_VAL_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BindPropCmd.DEL_BY_VAL_IDS);
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

    public int getRlPdByPropVal(int aid, int tid, int unionPriId, int sysType, FaiList<Param> propAndValList, FaiList<Integer> rlPdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            rlPdIds.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindPropDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductBindPropDto.Key.TID, tid);
            propAndValList.toBuffer(sendBody, ProductBindPropDto.Key.INFO_LIST, ProductBindPropDto.getInfoDto());
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindPropCmd.GET_LIST_BY_PROP);
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
            m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
            if (m_rt != Errno.OK || keyRef.value != ProductBindPropDto.Key.RL_PD_IDS) {
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

    /**
     * 根据业务商品id，获取商品业务关系数据
     */
    public int getRelInfoByRlId(int aid, int unionPriId, int sysType, int rlPdId, Param pdRelInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid <= 0 || pdRelInfo == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid=%d;uid=%d;rlPdId=%d;get args error;", aid, unionPriId, rlPdId);
                return m_rt;
            }

            pdRelInfo.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductRelDto.Key.RL_PD_ID, rlPdId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_REL);
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

            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = pdRelInfo.fromBuffer(recvBody, keyRef, ProductRelDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO) {
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

    /**
     * 根据商品业务id，获取商品数据
     * 商品表+业务关系表+各个绑定数据
     */
    public int getProductInfo(int aid, int unionPriId, int sysType, int rlPdId, Param info) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid <= 0 || info == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid=%d;uid=%d;rlPdId=%d;get args error;", aid, unionPriId, rlPdId);
                return m_rt;
            }

            info.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductRelDto.Key.RL_PD_ID, rlPdId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_PD_INFO);
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

            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = info.fromBuffer(recvBody, keyRef, ProductRelDto.getRelAndPdDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO) {
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

    /**
     * 根据商品业务id，获取商品数据
     * 商品表+业务关系表+各个绑定数据
     */
    public int getPdListByPdIds(int aid, int unionPriId, FaiList<Integer> pdIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid <= 0 || pdIds == null || list == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid=%d;uid=%d;pdIds=%s;get args error;", aid, unionPriId, pdIds);
                return m_rt;
            }

            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            pdIds.toBuffer(sendBody, ProductRelDto.Key.PD_IDS);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_LIST_BY_PDID);
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

            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getRelAndPdDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO_LIST) {
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

    /**
     * 根据业务商品id，获取商品业务关系数据
     */
    public int getPdInfoByPdId(int aid, int unionPriId, int pdId, Param info) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid <= 0 || info == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid=%d;uid=%d;pdId=%d;get args error;", aid, unionPriId, pdId);
                return m_rt;
            }

            info.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.PD_ID, pdId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_BY_PDID);
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

            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = info.fromBuffer(recvBody, keyRef, ProductRelDto.getRelAndPdDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO) {
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

    /**
     * 根据业务商品id集合，获取商品业务关系数据集合
     */
    public int getRelListByRlIds(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        return getRelListByRlIds(aid, unionPriId, sysType, rlPdIds, false, list);
    }
    public int getRelListByRlIds(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, boolean withSoftDel, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid <= 0 || list == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid=%d;uid=%d;list=%s;get args error;", aid, unionPriId, list);
                return m_rt;
            }
            if(rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "get args error, rlPdIds is null;aid=%d;uid=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
                return m_rt;
            }
            list.clear();

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            sendBody.putBoolean(ProductRelDto.Key.SOFT_DEL, withSoftDel);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_REL_LIST);
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

            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO_LIST) {
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

    /**
     * 根据pdIds获取业务关联数据，仅获取有限的字段，aid+unionPriId+pdId+rlPdId
     */
    public int getReducedRelsByPdIds(int aid, int unionPriId, FaiList<Integer> pdIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid <= 0 || list == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid=%d;uid=%d;list=%s;get args error;", aid, unionPriId, list);
                return m_rt;
            }
            if(pdIds == null || pdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "get args error, pdIds is null;aid=%d;uid=%d;pdIds=%s;", aid, unionPriId, pdIds);
                return m_rt;
            }
            list.clear();

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            pdIds.toBuffer(sendBody, ProductRelDto.Key.PD_IDS);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_REDUCED_REL_LIST);
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

            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getReducedInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.REDUCED_INFO) {
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

    /**
     * 根据name获取业务关联数据，仅获取有限的字段，name+pdId+rlPdId+status+sysType
     */
    public int getPdReducedList4Adm(int aid, int unionPriId, Integer sysType, FaiList<String> names, FaiList<Integer> status, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid <= 0 || list == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid=%d;uid=%d;list=%s;get args error;", aid, unionPriId, list);
                return m_rt;
            }
            if(names == null || names.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "get args error, names is null;aid=%d;uid=%d;names=%s;", aid, unionPriId, names);
                return m_rt;
            }
            list.clear();

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            if(sysType != null) {
                sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            }
            names.toBuffer(sendBody, ProductRelDto.Key.NAME);
            if(status != null) {
                status.toBuffer(sendBody, ProductRelDto.Key.STATUS);
            }
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_REDUCED_LIST_BY_NAME);
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

            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getReducedInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.REDUCED_INFO) {
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

    /**
     * 新增商品数据，并添加与当前unionPriId的关联
     */
    public int addProductAndRel(int aid, int tid, int siteId, int unionPriId, String xid, Param info, Ref<Integer> pdIdRef, Ref<Integer> rlPdIdRef) {
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
            if(xid != null) {
                sendBody.putString(ProductRelDto.Key.XID, xid);
            }
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            info.toBuffer(sendBody, ProductRelDto.Key.INFO, ProductRelDto.getRelAndPdDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.ADD_PD_AND_REL);
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

            if(pdIdRef != null && rlPdIdRef != null) {
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlPdIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.RL_PD_ID) {
                    Log.logErr(m_rt, "recv sid codec err");
                    return m_rt;
                }

                m_rt = recvBody.getInt(keyRef, pdIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.PD_ID) {
                    Log.logErr(m_rt, "recv sid codec err");
                    return m_rt;
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量新增商品数据，并添加与当前unionPriId的关联
     */
    public int batchAddProductAndRel(int aid, int tid, int siteId, int unionPriId, FaiList<Param> list, FaiList<Param> idInfoList) {
        return batchAddProductAndRel(aid, null, tid, siteId, unionPriId, list, idInfoList);
    }
    public int batchAddProductAndRel(int aid, String xid, int tid, int siteId, int unionPriId, FaiList<Param> list, FaiList<Param> idInfoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (list == null || list.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null;aid=%d;tid=%d;", aid, tid);
                return m_rt;
            }
            idInfoList.clear();

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductRelDto.Key.XID, xid);
            }
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            list.toBuffer(sendBody, ProductRelDto.Key.INFO, ProductRelDto.getRelAndPdDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.BATCH_ADD_PD_AND_REL);
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
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }

            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = idInfoList.fromBuffer(recvBody, keyRef, ProductRelDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 新增商品业务关联
     */
    public int bindProductRel(int aid, int tid, int unionPriId, String xid, Param bindRlPdInfo, Param info, Ref<Integer> rlPdIdRef, Ref<Integer> pdIdRef, Ref<Boolean> existRef) {
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
            if(xid != null) {
                sendBody.putString(ProductRelDto.Key.XID, xid);
            }
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            bindRlPdInfo.toBuffer(sendBody, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
            info.toBuffer(sendBody, ProductRelDto.Key.INFO, ProductRelDto.getRelAndPdDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.ADD_REL_BIND);
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

            if (rlPdIdRef != null) {
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.ERROR;
                    Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                    return m_rt;
                }
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlPdIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.RL_PD_ID) {
                    Log.logErr(m_rt, "recv rlPdId codec err");
                    return m_rt;
                }
                if(pdIdRef != null) {
                    m_rt = recvBody.getInt(keyRef, pdIdRef);
                    if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.PD_ID) {
                        Log.logErr(m_rt, "recv pdId codec err");
                        return m_rt;
                    }
                    if(existRef != null) {
                        m_rt = recvBody.getBoolean(keyRef, existRef);
                        if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.EXIST) {
                            Log.logErr(m_rt, "recv exist codec err");
                            return m_rt;
                        }
                    }
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量新增商品业务关联
     */
    public int batchBindProductRel(int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList) {
        return batchBindProductRel(aid, null, tid, bindRlPdInfo, infoList, null, null, null);
    }

    /**
     * 批量新增商品业务关联
     */
    public int batchBindProductRel(int aid, String xid, int tid, Param bindRlPdInfo, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef, Ref<Integer> pdIdRef, Ref<FaiList<Integer>> existListRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null || infoList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            if(xid == null) {
                xid = "";
            }
            sendBody.putString(ProductRelDto.Key.XID, xid);
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            bindRlPdInfo.toBuffer(sendBody, ProductRelDto.Key.INFO, ProductRelDto.getInfoDto());
            infoList.toBuffer(sendBody, ProductRelDto.Key.INFO_LIST, ProductRelDto.getRelAndPdDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.BATCH_ADD_REL_BIND);
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

            if (rlPdIdsRef != null) {
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.ERROR;
                    Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                    return m_rt;
                }
                FaiList<Integer> rlPdIds = new FaiList<>();
                Ref<Integer> keyRef = new Ref<>();
                m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.RL_PD_IDS) {
                    Log.logErr(m_rt, "recv rlPdIds codec err");
                    return m_rt;
                }
                rlPdIdsRef.value = rlPdIds;

                if(pdIdRef != null) {
                    m_rt = recvBody.getInt(keyRef, pdIdRef);
                    if(m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.PD_ID) {
                        Log.logErr(m_rt, "recv pdId codec err");
                        return m_rt;
                    }
                    if(existListRef != null) {
                        FaiList<Integer> existIds = new FaiList<>();
                        m_rt = existIds.fromBuffer(recvBody, keyRef);
                        if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.EXIST) {
                            Log.logErr(m_rt, "recv existIds codec err");
                            return m_rt;
                        }
                        existListRef.value = existIds;
                    }
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量新增多个商品业务关联
     */
    public int batchBindProductsRel(int aid, String xid, int tid, int toUnionPriId, int fromUnionPriId, int sysType, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef, Ref<FaiList<Integer>> pdIdsRef, Ref<FaiList<Integer>> existListRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null || infoList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            if(xid == null) {
                xid = "";
            }
            sendBody.putString(ProductRelDto.Key.XID, xid);
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, toUnionPriId);
            sendBody.putInt(ProductRelDto.Key.FROM_UNION_PRI_ID, fromUnionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            infoList.toBuffer(sendBody, ProductRelDto.Key.INFO_LIST, ProductRelDto.getRelAndPdDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.BATCH_BIND_PDS_REL);
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

            if (rlPdIdsRef != null) {
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.ERROR;
                    Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                    return m_rt;
                }
                FaiList<Integer> rlPdIds = new FaiList<>();
                Ref<Integer> keyRef = new Ref<>();
                m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.RL_PD_IDS) {
                    Log.logErr(m_rt, "recv rlPdIds codec err");
                    return m_rt;
                }
                rlPdIdsRef.value = rlPdIds;

                if(pdIdsRef != null) {
                    FaiList<Integer> pdIds = new FaiList<>();
                    m_rt = pdIds.fromBuffer(recvBody, keyRef);
                    if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.PD_IDS) {
                        Log.logErr(m_rt, "recv existIds codec err");
                        return m_rt;
                    }
                    pdIdsRef.value = pdIds;
                    if(existListRef != null) {
                        FaiList<Integer> existIds = new FaiList<>();
                        m_rt = existIds.fromBuffer(recvBody, keyRef);
                        if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.EXIST) {
                            Log.logErr(m_rt, "recv existIds codec err");
                            return m_rt;
                        }
                        existListRef.value = existIds;
                    }
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int batchBindProductsRel(int aid, String xid, int tid, boolean needSyncInfo, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null || infoList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductRelDto.Key.XID, xid);
            }
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putBoolean(ProductRelDto.Key.NEED_SYNC_INFO, needSyncInfo);
            infoList.toBuffer(sendBody, ProductRelDto.Key.INFO_LIST, ProductRelDto.getTmpBindDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.BATCH_ADD_PDS_REL_BIND);
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

    /**
     * 删除商品数据，同时删除所有相关业务关联数据
     */
    public int batchDelProduct(int aid, String xid, int tid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, boolean softDel) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIds is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductRelDto.Key.XID, xid);
            }
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            sendBody.putBoolean(ProductRelDto.Key.SOFT_DEL, softDel);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.DEL_PDS);
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

    public int clearRelData(int aid, int unionPriId, boolean softDel) {
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
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putBoolean(ProductRelDto.Key.SOFT_DEL, softDel);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.CLEAR_REL_DATA);
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

    public int clearAcct(int aid, FaiList<Integer> unionPriIds) {
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
            unionPriIds.toBuffer(sendBody, ProductRelDto.Key.UNION_PRI_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.CLEAR_ACCT);
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

    /**
     * 取消商品业务关联
     * softDel: 是否软删除
     */
    public int batchDelPdRelBind(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, boolean softDel) {
        return batchDelPdRelBind(aid, unionPriId, "", sysType, rlPdIds, softDel);
    }
    public int batchDelPdRelBind(int aid, int unionPriId, String xid, int sysType, FaiList<Integer> rlPdIds, boolean softDel) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIds is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putString(ProductRelDto.Key.XID, xid);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            sendBody.putBoolean(ProductRelDto.Key.SOFT_DEL, softDel);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.DEL_REL_BIND);
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

    public int batchSet4YK(int aid, String xid, int ownUnionPriId, int sysType, FaiList<Integer> unionPriIds, FaiList<Integer> rlPdIds, ParamUpdater updater, FaiList<Param> addList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(unionPriIds == null || rlPdIds == null || addList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;ownUid=%d;uids=%s;sysType=%s;rlPdIds=%s;updater=%s;", addList, ownUnionPriId, unionPriIds, sysType, rlPdIds, updater.toJson());
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putString(ProductRelDto.Key.XID, xid);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, ownUnionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            unionPriIds.toBuffer(sendBody, ProductRelDto.Key.UNION_PRI_IDS);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            updater.toBuffer(sendBody, ProductRelDto.Key.UPDATER, ProductRelDto.getRelAndPdDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.BATCH_SET_4YK);
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
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = addList.fromBuffer(recvBody, keyRef, ProductRelDto.getReducedInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.REDUCED_INFO) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int cloneBizBind(int aid, int fromUnionPriId, int toUnionPriId, boolean silentDel) {
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
            sendBody.putInt(ProductRelDto.Key.FROM_UNION_PRI_ID, fromUnionPriId);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, toUnionPriId);
            sendBody.putBoolean(ProductRelDto.Key.SOFT_DEL, silentDel);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.CLONE_BIZ_BIND);
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

    /**
     * 修改sort
     */
    public int setPdSort(int aid, int tid, int unionPriId, int sysType, Integer rlPdId, Integer preRlPdId) {
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
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductRelDto.Key.RL_PD_ID, rlPdId);
            sendBody.putInt(ProductRelDto.Key.PRE_RL_PD_ID, preRlPdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.SET_SORT);
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

    /**
     * 修改单个商品数据
     */
    public int setSinglePd(int aid, String xid, int tid, int siteId, int unionPriId, int sysType, Integer rlPdId, ParamUpdater updater) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (updater == null || updater.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updater is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            if(!Str.isEmpty(xid)) {
                sendBody.putString(ProductRelDto.Key.XID, xid);
            }
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductRelDto.Key.RL_PD_ID, rlPdId);
            updater.toBuffer(sendBody, ProductRelDto.Key.UPDATER, ProductRelDto.getRelAndPdDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.SET_SINGLE_PD);
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

    /**
     * 修改商品数据
     */
    public int setProducts(int aid, int tid, int siteId, int unionPriId, int sysType, FaiList<Integer> rlPdIds, ParamUpdater updater) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIds is empty");
                return m_rt;
            }

            if (updater == null || updater.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updater is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.TID, tid);
            sendBody.putInt(ProductRelDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            updater.toBuffer(sendBody, ProductRelDto.Key.UPDATER, ProductRelDto.getRelAndPdDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.SET_PDS);
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

    /**
     * 获取商品数据状态
     */
    public int getPdDataStatus(int aid, Param statusInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            statusInfo.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.PD_DATA_STATUS);
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
            if (m_rt != Errno.OK || keyRef.value != ProductDto.Key.DATA_STATUS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 从db查询商品数据
     */
    public int searchPdFromDb(int aid, SearchArg searchArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            searchArg.toBuffer(sendBody, ProductDto.Key.SEARCH_ARG);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.SEARCH_PD_FROM_DB);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductDto.Key.TOTAL_SIZE) {
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

    /**
     * 获取aid + unionPriId 下所有商品数据
     */
    public int getAllPdData(int aid, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_ALL_PD);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductDto.Key.INFO_LIST) {
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

    /**
     * 获取商品关联数据状态
     */
    public int getPdRelDataStatus(int aid, int unionPriId, Param statusInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            statusInfo.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.PD_REL_DATA_STATUS);
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
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.DATA_STATUS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 从db查询商品关联数据
     */
    public int searchPdRelFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, ProductRelDto.Key.SEARCH_ARG);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.SEARCH_PD_REL_FROM_DB);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductRelDto.Key.TOTAL_SIZE) {
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

    /**
     * 获取aid + unionPriId 下所有商品关联数据
     */
    public int getAllPdRelData(int aid, int unionPriId, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_ALL_PD_REL);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO_LIST) {
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

    /**
     * 获取商品参数关联数据状态
     */
    public int getBindPropDataStatus(int aid, int unionPriId, Param statusInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            statusInfo.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindPropCmd.GET_DATA_STATUS);
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
            if (m_rt != Errno.OK || keyRef.value != ProductBindPropDto.Key.DATA_STATUS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 从db查询商品参数关联数据
     */
    public int searchBindPropFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, ProductBindPropDto.Key.SEARCH_ARG);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindPropCmd.SEARCH_FROM_DB);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBindPropDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindPropDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductBindPropDto.Key.TOTAL_SIZE) {
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

    /**
     * 获取aid + unionPriId 下所有商品参数关联数据
     */
    public int getAllBindPropData(int aid, int unionPriId, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindPropCmd.GET_ALL_DATA);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBindPropDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindPropDto.Key.INFO_LIST) {
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

    public int getPdBindGroup(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindGroupDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindGroupDto.Key.SYS_TYPE, sysType);
            rlPdIds.toBuffer(sendBody,ProductBindGroupDto.Key.RL_PD_IDS);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindGroupCmd.GET_LIST);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBindGroupDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindGroupDto.Key.INFO_LIST) {
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

    public int setPdBindGroup(int aid, int unionPriId, int sysType, int rlPdId, FaiList<Integer> addGroupIds, FaiList<Integer> delGroupIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(addGroupIds == null) {
                addGroupIds = new FaiList<Integer>();
            }
            if(delGroupIds == null) {
                delGroupIds = new FaiList<Integer>();
            }
            if(addGroupIds.isEmpty() && delGroupIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList and delList all empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindGroupDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindGroupDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductBindGroupDto.Key.RL_PD_ID, rlPdId);
            addGroupIds.toBuffer(sendBody, ProductBindGroupDto.Key.RL_GROUP_IDS);
            delGroupIds.toBuffer(sendBody, ProductBindGroupDto.Key.DEL_RL_GROUP_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BindGroupCmd.BATCH_SET);
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

    public int delPdBindGroup(int aid, int unionPriId, int sysType, FaiList<Integer> delGroupIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(delGroupIds == null || delGroupIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;delList is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindGroupDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindGroupDto.Key.SYS_TYPE, sysType);
            delGroupIds.toBuffer(sendBody, ProductBindGroupDto.Key.DEL_RL_GROUP_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.BindGroupCmd.DEL);
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

    public int getRlPdByRlGroupId(int aid, int unionPriId, int sysType, FaiList<Integer> rlGroupIds, FaiList<Integer> rlPdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            rlPdIds.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindGroupDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindGroupDto.Key.SYS_TYPE, sysType);
            rlGroupIds.toBuffer(sendBody, ProductBindGroupDto.Key.RL_GROUP_IDS);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindGroupCmd.GET_PD_BY_GROUP);
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
            m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
            if (m_rt != Errno.OK || keyRef.value != ProductBindGroupDto.Key.RL_PD_IDS) {
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

    /**
     * 根据商品业务id获取商品数据（商品表+业务表）
     */
    public int getProductList(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_PD_LIST);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getRelAndPdDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO_LIST) {
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

    /**
     * 根据商品业务id获取商品绑定了哪些业务
     */
    public int getPdBindBiz(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BasicCmd.GET_BIND_BIZ);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getPdBindBizDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO_LIST) {
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

    /**
     * 获取商品分类关联数据状态
     */
    public int getBindGroupDataStatus(int aid, int unionPriId, Param statusInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            statusInfo.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindGroupDto.Key.UNION_PRI_ID, unionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindGroupCmd.GET_DATA_STATUS);
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
            if (m_rt != Errno.OK || keyRef.value != ProductBindGroupDto.Key.DATA_STATUS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

    /**
     * 从db查询商品分类关联数据
     */
    public int searchBindGroupFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindGroupDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, ProductBindGroupDto.Key.SEARCH_ARG);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindGroupCmd.SEARCH_FROM_DB);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBindGroupDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindGroupDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductBindGroupDto.Key.TOTAL_SIZE) {
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

    /**
     * 获取aid + unionPriId 下所有商品分类关联数据
     */
    public int getAllBindGroupData(int aid, int unionPriId, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindGroupDto.Key.UNION_PRI_ID, unionPriId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductBasicCmd.BindGroupCmd.GET_ALL_DATA);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBindGroupDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindGroupDto.Key.INFO_LIST) {
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

    /**==========================================操作商品与标签关联开始===========================================================*/
    /**
     * 获取aid，uid，rlPdIds获取商品和标签关联的数据
     */
    public int getPdBindTag(int aid, int unionPriId, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> pdBindTags) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (pdBindTags == null) {
                pdBindTags = new FaiList<Param>();
            }
            pdBindTags.clear();

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindTagDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindTagDto.Key.SYS_TYPE, sysType);
            rlPdIds.toBuffer(sendBody,ProductBindTagDto.Key.RL_PD_IDS);
            //send and receive
            Param result = sendAndReceive(aid, MgProductBasicCmd.BindTagCmd.GET_LIST, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            // recv info
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = pdBindTags.fromBuffer(recvBody, keyRef, ProductBindTagDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindTagDto.Key.INFO_LIST) {
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

    public int setPdBindTag(int aid, int unionPriId, int sysType, int rlPdId, FaiList<Integer> addRlTagIds, FaiList<Integer> delRlTagIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr("args error");
                return m_rt;
            }
            if(addRlTagIds == null) {
                addRlTagIds = new FaiList<Integer>();
            }
            if(delRlTagIds == null) {
                delRlTagIds = new FaiList<Integer>();
            }
            if(addRlTagIds.isEmpty() && delRlTagIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList and delList all empty");
                return m_rt;
            }

            int command = MgProductBasicCmd.BindTagCmd.BATCH_SET;
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindTagDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindTagDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductBindTagDto.Key.RL_PD_ID, rlPdId);
            addRlTagIds.toBuffer(sendBody, ProductBindTagDto.Key.RL_TAG_IDS);
            delRlTagIds.toBuffer(sendBody, ProductBindTagDto.Key.DEL_RL_TAG_IDS);

            sendAndReceive(aid, command, sendBody, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int delPdBindTag(int aid, int unionPriId, int sysType, FaiList<Integer> delRlPdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(Util.isEmptyList(delRlPdIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;delList is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindTagDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindTagDto.Key.SYS_TYPE, sysType);
            delRlPdIds.toBuffer(sendBody, ProductBindTagDto.Key.RL_PD_IDS);

            //send and receive
            sendAndReceive(aid, MgProductBasicCmd.BindTagCmd.DEL, sendBody, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);

        }
    }

    public int getRlPdIdsByRlTagIds(int aid, int unionPriId, int sysType, FaiList<Integer> rlTagIds, FaiList<Integer> rlPdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if(Util.isEmptyList(rlTagIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;delList is empty");
                return m_rt;
            }
            if (rlPdIds == null) {
                rlPdIds = new FaiList<Integer>();
            }
            rlPdIds.clear();

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindTagDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductBindTagDto.Key.SYS_TYPE, sysType);
            rlTagIds.toBuffer(sendBody, ProductBindTagDto.Key.RL_TAG_IDS);

            Param result = sendAndReceive(aid, MgProductBasicCmd.BindTagCmd.GET_PD_BY_TAG, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            // recv info
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
            if (m_rt != Errno.OK || keyRef.value != ProductBindTagDto.Key.RL_PD_IDS) {
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

    public int getBindTagDataStatus(int aid, int unionPriId, Param statusInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            statusInfo.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindTagDto.Key.UNION_PRI_ID, unionPriId);

            Param result = sendAndReceive(aid, MgProductBasicCmd.BindTagCmd.GET_DATA_STATUS, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            // recv info
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = statusInfo.fromBuffer(recvBody, keyRef, DataStatus.Dto.getDataStatusDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindTagDto.Key.DATA_STATUS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);

        }
    }

    public int getAllPdBindTag(int aid, int unionPriId, FaiList<Param> list) {
        return getPdBindTagByCondition(aid, unionPriId, null, list);
    }

    public int getPdBindTagFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> list) {
        if (searchArg == null) {
            searchArg = new SearchArg();
        }
        return getPdBindTagByCondition(aid, unionPriId, searchArg, list);
    }

    private int getPdBindTagByCondition(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> pdBindTags) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (pdBindTags == null) {
                pdBindTags = new FaiList<Param>();
            }
            pdBindTags.clear();
            int command = MgProductBasicCmd.BindTagCmd.GET_ALL_DATA;
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindTagDto.Key.UNION_PRI_ID, unionPriId);

            if (searchArg != null) {
                command = MgProductBasicCmd.BindTagCmd.SEARCH_FROM_DB;
                searchArg.toBuffer(sendBody, ProductBindTagDto.Key.SEARCH_ARG);
            }else {
                searchArg = new SearchArg();
            }

            Param result = sendAndReceive(aid, command, sendBody,true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            // recv info
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = pdBindTags.fromBuffer(recvBody, keyRef, ProductBindTagDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBindTagDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductBindTagDto.Key.TOTAL_SIZE) {
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

    /**
     * 发送和接收数据，并且验证发送和接收是否成功
     *
     * @param sendBody          发送的数据体
     * @param verifyReceiveBody true表示要验证接收的数据是否为null，false表示不用验证
     * @return true 表示发送和接收成功，false表示发送和接收失败
     */
    private Param sendAndReceive(int aid, int command, FaiBuffer sendBody, boolean verifyReceiveBody) {

        Param param = new Param(true);
        FaiProtocol sendProtocol = new FaiProtocol();
        sendProtocol.setAid(aid);
        sendProtocol.setCmd(command);
        sendProtocol.addEncodeBody(sendBody);
        //send
        m_rt = send(sendProtocol);
        if (m_rt != Errno.OK) {
            Log.logErr(m_rt, "send err");
            return param.setBoolean("success", false);
        }

        // recv
        FaiProtocol recvProtocol = new FaiProtocol();
        m_rt = recv(recvProtocol);
        if (m_rt != Errno.OK) {
            Log.logErr(m_rt, "recv err");
            return param.setBoolean("success", false);
        }
        m_rt = recvProtocol.getResult();
        if (m_rt != Errno.OK) {
            if (m_rt != Errno.NOT_FOUND) {
                Log.logErr(m_rt, "recv result err");
            }
            return param.setBoolean("success", false);
        }

        if (!verifyReceiveBody) {
            return param.setBoolean("success", true);
        }

        FaiBuffer recvBody = recvProtocol.getDecodeBody();
        if (recvBody == null) {
            m_rt = Errno.CODEC_ERROR;
            Log.logErr(m_rt, "recv body null");
            return param.setBoolean("success", false);
        }
        return param.setBoolean("success", true).setObject("recvBody", recvBody);
    }
    /**==========================================操作商品与标签关联结束===========================================================*/

    /*public int dataMigrate(int aid, int tid, FaiList<Param> addList, int sysType, FaiList<Param> returnList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || addList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;");
                return m_rt;
            }
            if(addList == null || addList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList and delList all empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductDto.Key.TID, tid);
            addList.toBuffer(sendBody, ProductDto.Key.INFO_LIST, MigrateDef.Dto.getInfoDto());
            sendBody.putInt(ProductDto.Key.SYS_TYPE, sysType);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.Cmd.MIGRATE);
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
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = returnList.fromBuffer(recvBody, keyRef, ProductRelDto.getReducedInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.REDUCED_INFO) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }*/

    /*
    public int getMigratePdIds(int aid, int sysType, FaiList<Integer> pdIds, boolean needDel) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductDto.Key.SYS_TYPE, sysType);
            sendBody.putBoolean(ProductDto.Key.DEL, needDel);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductBasicCmd.Cmd.MIGRATE_GET);
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
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = pdIds.fromBuffer(recvBody, keyRef);
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.PD_IDS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }*/

    /**
     * 恢复软删除数据
     */
    public int restoreData(int aid, int unionPriId, String xid, FaiList<Integer> rlPdIds, int sysType, FaiList<Integer> pdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
                return m_rt;
            }

            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putString(ProductRelDto.Key.XID, xid);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);

            Param result = sendAndReceive(aid, MgProductBasicCmd.Cmd.RESTORE_DATA, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            // recv info
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            Ref<Integer> keyRef = new Ref<>();
            m_rt = pdIds.fromBuffer(recvBody, keyRef);
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.PD_IDS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int getListByUidsAndRlPdIds(int aid, FaiList<Integer> unionPriIdList, FaiList<Integer> rlPdIds, int sysType, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || rlPdIds == null || rlPdIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;unionPriIdList=%s;rlPdId=%s;", aid, unionPriIdList, rlPdIds);
                return m_rt;
            }

            FaiBuffer sendBody = new FaiBuffer(true);
            unionPriIdList.toBuffer(sendBody, ProductRelDto.Key.UNION_PRI_IDS);
            rlPdIds.toBuffer(sendBody, ProductRelDto.Key.RL_PD_IDS);
            sendBody.putInt(ProductRelDto.Key.SYS_TYPE, sysType);

            Param result = sendAndReceive(aid, MgProductBasicCmd.Cmd.GET_PRI_LIST, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }
            // recv info
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            Ref<Integer> keyRef = new Ref<>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductRelDto.getRelAndPdDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.INFO_LIST) {
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

    public int getRlPdIdAndPdIdMap(int aid, int fromAid, int toUnionPriId, int toSysType, int fromSysType, int fromUnionPriId, FaiList<Param> idMap, FaiList<Param> toRlPdIdAndPdIdList, FaiList<Param> fromRlPdIdAndPdIdList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (idMap == null || idMap.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;idMap is empty");
                return m_rt;
            }

            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductDto.Key.FROM_AID, fromAid);
            sendBody.putInt(ProductDto.Key.TO_UNION_PRI_ID, toUnionPriId);
            sendBody.putInt(ProductDto.Key.TO_SYS_TYPE, toSysType);
            sendBody.putInt(ProductDto.Key.FROM_SYS_TYPE, fromSysType);
            sendBody.putInt(ProductDto.Key.FROM_UNION_PRI_ID, fromUnionPriId);

            idMap.toBuffer(sendBody, ProductDto.Key.RL_PD_ID_MAP, ProductDto.getRlPdIdDto());

            Param result = sendAndReceive(aid, MgProductBasicCmd.Cmd.GET_PD_ID_MAP, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }
            // recv info
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            Ref<Integer> keyRef = new Ref<>();
            m_rt = toRlPdIdAndPdIdList.fromBuffer(recvBody, keyRef, ProductRelDto.getRelAndPdDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.TO_RL_PD_ID_AND_PD_ID) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            m_rt = fromRlPdIdAndPdIdList.fromBuffer(recvBody, keyRef, ProductRelDto.getRelAndPdDto());
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.FROM_RL_PD_ID_AND_PD_ID) {
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

    public int getSoftDelUnionPriIdList(int aid, int pdId, FaiList<Integer> unionPriIds, FaiList<Integer> softDelUnionPriIdList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductRelDto.Key.PD_ID, pdId);
            unionPriIds.toBuffer(sendBody, ProductRelDto.Key.UNION_PRI_IDS);

            Param result = sendAndReceive(aid, MgProductBasicCmd.BasicCmd.GET_SOFT_DEL_UNION_PRI_IDS, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }
            // recv info
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            Ref<Integer> keyRef = new Ref<>();
            m_rt = softDelUnionPriIdList.fromBuffer(recvBody, keyRef);
            if (m_rt != Errno.OK || keyRef.value != ProductRelDto.Key.UNION_PRI_IDS) {
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
}
