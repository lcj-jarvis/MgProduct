package fai.MgProductPropSvr.interfaces.cli;

import fai.MgProductPropSvr.interfaces.cmd.MgProductPropCmd;
import fai.MgProductPropSvr.interfaces.dto.ProductPropDto;
import fai.MgProductPropSvr.interfaces.dto.ProductPropValDto;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

public class MgProductPropCli extends FaiClient {

    public MgProductPropCli(int flow) {
        super(flow, "MgProductPropCli");
    }

    /**
     * <p> 初始化
     * @return
     */
    public boolean init() {
        return init("MgProductPropCli", true);
    }

    public int getPropList(int aid, int tid, int unionPriId, int libId, SearchArg searchArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            searchArg.toBuffer(sendBody, ProductPropDto.Key.SEARCH_ARG);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductPropCmd.PropCmd.GET_LIST);
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
            list.fromBuffer(recvBody, keyRef, ProductPropDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductPropDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductPropDto.Key.TOTAL_SIZE) {
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

    public int addPropList(int aid, int tid, int unionPriId, int libId, FaiList<Param> list, Ref<FaiList<Integer>> idsRef) {
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
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            list.toBuffer(sendBody, ProductPropDto.Key.INFO_LIST, ProductPropDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductPropCmd.PropCmd.BATCH_ADD);
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

            if(idsRef != null) {
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.ERROR;
                    Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                    return m_rt;
                }
                Ref<Integer> keyRef = new Ref<Integer>();
                list.fromBuffer(sendBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductPropDto.Key.RL_PROP_IDS) {
                    Log.logErr(m_rt, "recv rlPropIds codec err");
                    return m_rt;
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int addPropInfoWithVal(int aid, int tid, int unionPriId, int libId, Param propInfo, FaiList<Param> valList, Ref<Integer> rlPropIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (propInfo == null || propInfo.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "propInfo is null");
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            propInfo.toBuffer(sendBody, ProductPropDto.Key.INFO, ProductPropDto.getInfoDto());
            valList.toBuffer(sendBody, ProductPropValDto.Key.INFO_LIST, ProductPropValDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductPropCmd.PropCmd.ADD_WITH_VAL);
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

            if(rlPropIdRef != null) {
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.ERROR;
                    Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                    return m_rt;
                }
                Ref<Integer> keyRef = new Ref<Integer>();
                sendBody.getInt(keyRef, rlPropIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductPropDto.Key.RL_PROP_ID) {
                    Log.logErr(m_rt, "recv rlPropId codec err");
                    return m_rt;
                }
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int setPropList(int aid, int tid, int unionPriId, int libId, FaiList<ParamUpdater> updaterList) {
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
            sendBody.putInt(ProductPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            updaterList.toBuffer(sendBody, ProductPropDto.Key.UPDATERLIST, ProductPropDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductPropCmd.PropCmd.BATCH_SET);
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

    public int delPropList(int aid, int tid, int unionPriId, int libId, FaiList<Integer> idList) {
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
            sendBody.putInt(ProductPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            idList.toBuffer(sendBody, ProductPropDto.Key.RL_PROP_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductPropCmd.PropCmd.BATCH_DEL);
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

    public int getPropValList(int aid, int tid, int unionPriId, int libId, FaiList<Integer> rlPropIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            rlPropIds.toBuffer(sendBody, ProductPropValDto.Key.RL_PROP_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductPropCmd.PropValCmd.GET_LIST);
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
            list.fromBuffer(recvBody, keyRef, ProductPropValDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductPropValDto.Key.INFO_LIST) {
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

    public int setPropValList(int aid, int tid, int unionPriId, int libId, int rlPropId, FaiList<Param> addList, FaiList<ParamUpdater> setList, FaiList<Integer> delList) {
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
            if(setList == null) {
                setList = new FaiList<ParamUpdater>();
            }
            if(delList == null) {
                delList = new FaiList<Integer>();
            }
            if(addList.isEmpty() && setList.isEmpty() && delList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList, setList, delList all empty;aid=%d;", aid);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            sendBody.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropId);
            setList.toBuffer(sendBody, ProductPropValDto.Key.UPDATERLIST, ProductPropValDto.getInfoDto());
            delList.toBuffer(sendBody, ProductPropValDto.Key.VAL_IDS);
            addList.toBuffer(sendBody, ProductPropValDto.Key.INFO_LIST, ProductPropValDto.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductPropCmd.PropValCmd.BATCH_SET);
            sendProtocol.setAid(aid);
            sendProtocol.addEncodeBody(sendBody);
            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
}
