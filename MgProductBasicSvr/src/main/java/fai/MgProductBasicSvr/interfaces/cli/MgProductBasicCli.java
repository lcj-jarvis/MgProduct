package fai.MgProductBasicSvr.interfaces.cli;

import fai.MgProductBasicSvr.interfaces.cmd.MgProductBasicCmd;
import fai.MgProductBasicSvr.interfaces.dto.ProductBindPropDto;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

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

    public int getPdBindProp(int aid, int tid, int unionPriId, int rlPdId, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
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
            list.fromBuffer(recvBody, keyRef, ProductBindPropDto.getInfoDto());
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

    public int setPdBindProp(int aid, int tid, int unionPriId, int rlPdId, FaiList<Param> addList, FaiList<Param> delList) {
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

    public int getRlPdByPropVal(int aid, int tid, int unionPriId, FaiList<Param> propAndValList, FaiList<Integer> rlPdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            rlPdIds.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBindPropDto.Key.UNION_PRI_ID, unionPriId);
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
            rlPdIds.fromBuffer(recvBody, keyRef);
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
}
