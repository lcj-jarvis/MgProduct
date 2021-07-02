package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.comm.util.*;

// 对外统一提供的接口类,接口都在各个父类中
public class MgProductInfCli extends MgProductInfCli6ForProductLib {
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
}