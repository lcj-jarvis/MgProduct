package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.entity.MgProductEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.comm.middleground.app.CloneDef;
import fai.comm.util.*;

// 对外统一提供的接口类,接口都在各个父类中
public class MgProductInfCli extends MgProductInfCli7ForProductTag {
    public MgProductInfCli(int flow) {
        super(flow);
    }

    public int clearRelData(MgProductArg mgProductArg){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.CLEAR_REL_DATA, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int clearAcct(MgProductArg mgProductArg){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> primaryKeys = mgProductArg.getPrimaryKeys();
            if(primaryKeys == null || primaryKeys.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;primaryKeys is null;aid=%d;", aid);
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.TID, tid), new Pair(MgProductDto.Key.SITE_ID, siteId), new Pair(MgProductDto.Key.LGID, lgId), new Pair(MgProductDto.Key.KEEP_PRIID1, keepPriId1));
            primaryKeys.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEYS, MgProductDto.getPrimaryKeyDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.CLEAR_ACCT, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 克隆数据
     * @param mgProductArg
     *
     * @return
     */
    public int cloneData(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> primaryKeys = mgProductArg.getPrimaryKeys();
            if(primaryKeys == null || primaryKeys.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;primaryKeys is null;aid=%d;", aid);
                return m_rt;
            }
            Param cloneOption = mgProductArg.getOption();

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer();
            primaryKey.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            sendBody.putInt(MgProductDto.Key.FROM_AID, mgProductArg.getFromAid());
            primaryKeys.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEYS, CloneDef.Dto.getExternalDto());
            if(cloneOption != null) {
                cloneOption.toBuffer(sendBody, MgProductDto.Key.OPTION, MgProductDto.getOptionDto());
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.CLONE_DATA, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 增量克隆
     * @param mgProductArg
     * @return
     */
    public int incrementalClone(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            Param fromPrimaryKey = mgProductArg.getFromPrimaryKey();
            if(Str.isEmpty(fromPrimaryKey)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;fromPrimaryKey is null;aid=%d;", aid);
                return m_rt;
            }
            Param cloneOption = mgProductArg.getOption();

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer();
            primaryKey.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            sendBody.putInt(MgProductDto.Key.FROM_AID, mgProductArg.getFromAid());
            fromPrimaryKey.toBuffer(sendBody, MgProductDto.Key.FROM_PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            if(cloneOption != null) {
                cloneOption.toBuffer(sendBody, MgProductDto.Key.OPTION, MgProductDto.getOptionDto());
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.INC_CLONE, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 克隆业务绑定数据
     * for 门店通新增门店场景
     * 克隆
     *  1. 基础服务业务关联表数据
     *  2. 库存销售数据，库存相关会初始化为0
     * @param mgProductArg
     * @return
     */
    public int cloneBizBInd(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            Param fromPrimaryKey = mgProductArg.getFromPrimaryKey();
            if(Str.isEmpty(fromPrimaryKey)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;fromPrimaryKey is null;aid=%d;", aid);
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer();
            primaryKey.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            fromPrimaryKey.toBuffer(sendBody, MgProductDto.Key.FROM_PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.CLONE_BIZ_BIND, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 备份
     * @param mgProductArg
     * @return
     */
    public int backupData(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> backupPrimaryKeys = mgProductArg.getPrimaryKeys();
            if(backupPrimaryKeys == null || backupPrimaryKeys.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;backupPrimaryKeys is null;aid=%d;", aid);
                return m_rt;
            }

            int rlBackupId = mgProductArg.getRlBackupId();
            if(rlBackupId <= 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlBackupId is err;aid=%d;rlBackupId=%d;", aid, rlBackupId);
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer();
            primaryKey.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            backupPrimaryKeys.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEYS, MgProductDto.getPrimaryKeyDto());
            sendBody.putInt(MgProductDto.Key.RL_BACKUPID, rlBackupId);

            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.BACKUP, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 还原
     * @param mgProductArg
     * @return
     */
    public int restoreBackupData(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> restorePrimaryKeys = mgProductArg.getPrimaryKeys();
            if(restorePrimaryKeys == null || restorePrimaryKeys.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;restorePrimaryKeys is null;aid=%d;", aid);
                return m_rt;
            }

            int rlRestoreId = mgProductArg.getRlBackupId();
            if(rlRestoreId <= 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlRestoreId is err;aid=%d;rlRestoreId=%d;", aid, rlRestoreId);
                return m_rt;
            }

            int rlBackupId = mgProductArg.getRlBackupId();
            if(rlBackupId <= 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlBackupId is err;aid=%d;rlBackupId=%d;", aid, rlBackupId);
                return m_rt;
            }

            Param restoreOption = mgProductArg.getOption();

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer();
            primaryKey.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            restorePrimaryKeys.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEYS, MgProductDto.getPrimaryKeyDto());
            sendBody.putInt(MgProductDto.Key.RL_RESTOREID, rlRestoreId);
            sendBody.putInt(MgProductDto.Key.RL_BACKUPID, rlBackupId);
            if(restoreOption != null) {
                restoreOption.toBuffer(sendBody, MgProductDto.Key.OPTION, MgProductDto.getOptionDto());
            }

            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.RESTORE, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 删除备份
     * @param mgProductArg
     * @return
     */
    public int delBackupData(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            int rlBackupId = mgProductArg.getRlBackupId();
            if(rlBackupId <= 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;rlBackupId is err;aid=%d;rlBackupId=%d;", aid, rlBackupId);
                return m_rt;
            }


            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param primaryKey = new Param();
            primaryKey.setInt(MgProductEntity.Info.TID, tid);
            primaryKey.setInt(MgProductEntity.Info.SITE_ID, siteId);
            primaryKey.setInt(MgProductEntity.Info.LGID, lgId);
            primaryKey.setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1);
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer();
            primaryKey.toBuffer(sendBody, MgProductDto.Key.PRIMARY_KEY, MgProductDto.getPrimaryKeyDto());
            sendBody.putInt(MgProductDto.Key.RL_BACKUPID, rlBackupId);

            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.DEL_BACKUP, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int getProduct4ES(int aid, int unionPriId, int pdId, Param info) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(MgProductDto.Key.UNIONPRI_ID, unionPriId), new Pair(MgProductDto.Key.PD_ID, pdId));
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.Cmd.GET_INFO_4ES, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = info.fromBuffer(recvBody, keyRef, MgProductDto.getEsPdInfoDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductDto.Key.INFO) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }
}