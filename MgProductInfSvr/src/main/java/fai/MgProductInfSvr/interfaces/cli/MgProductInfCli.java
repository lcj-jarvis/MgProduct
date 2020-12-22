package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductPropDto;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

import java.nio.ByteBuffer;

public class MgProductInfCli extends FaiClient {

    public MgProductInfCli(int flow) {
        super(flow, "MgProductInfCli");
    }

    public boolean init() {
        return init("MgProductInfCli", true);
    }

    public int getPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, SearchArg searchArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductPropDto.Key.LGID, lgId);
            sendBody.putInt(ProductPropDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            searchArg.toBuffer(sendBody, ProductPropDto.Key.SEARCH_ARG);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductInfCmd.PropCmd.GET_LIST);
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
            list.fromBuffer(recvBody, keyRef, ProductPropDto.getPropInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductPropDto.Key.PROP_LIST) {
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

    public int addPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Param> list, Ref<FaiList<Integer>> idsRef) {
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
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductPropDto.Key.LGID, lgId);
            sendBody.putInt(ProductPropDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            list.toBuffer(sendBody, ProductPropDto.Key.PROP_LIST, ProductPropDto.getPropInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.PropCmd.BATCH_ADD);
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

    public int setPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<ParamUpdater> updaterList){
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
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductPropDto.Key.LGID, lgId);
            sendBody.putInt(ProductPropDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            updaterList.toBuffer(sendBody, ProductPropDto.Key.UPDATERLIST, ProductPropDto.getPropInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.PropCmd.BATCH_SET);
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

    public int delPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> idList) {
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
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductPropDto.Key.LGID, lgId);
            sendBody.putInt(ProductPropDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            idList.toBuffer(sendBody, ProductPropDto.Key.RL_PROP_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.PropCmd.BATCH_DEL);
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

    public int addPropInfoWithVal(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, Param propInfo, FaiList<Param> propValList) {
        return addPropInfoWithVal(aid, tid, siteId, lgId, keepPriId1, libId, propInfo, propValList);
    }

    public int addPropInfoWithVal(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, Param propInfo, FaiList<Param> propValList, Ref<Integer> rlPropIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (propInfo == null) {
                propInfo = new Param();
            }
            if(propValList == null) {
                propValList = new FaiList<Param>();
            }
            if (propInfo.isEmpty() && propValList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "propInfo, propValList all null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductPropDto.Key.LGID, lgId);
            sendBody.putInt(ProductPropDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            propInfo.toBuffer(sendBody, ProductPropDto.Key.PROP_INFO, ProductPropDto.getPropInfoDto());
            propValList.toBuffer(sendBody, ProductPropDto.Key.VAL_LIST, ProductPropDto.getPropValInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.PropCmd.ADD_WITH_VAL);
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

    public int setPropAndVal(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, ParamUpdater propUpdater, FaiList<Param> addValList, FaiList<ParamUpdater> setValUpList, FaiList<Integer> delValIds) {
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
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductPropDto.Key.LGID, lgId);
            sendBody.putInt(ProductPropDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            sendBody.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropId);
            propUpdater.toBuffer(sendBody, ProductPropDto.Key.UPDATER, ProductPropDto.getPropInfoDto());
            addValList.toBuffer(sendBody, ProductPropDto.Key.VAL_LIST, ProductPropDto.getPropValInfoDto());
            setValUpList.toBuffer(sendBody, ProductPropDto.Key.UPDATERLIST, ProductPropDto.getPropValInfoDto());
            delValIds.toBuffer(sendBody, ProductPropDto.Key.VAL_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.PropCmd.SET_WITH_VAL);
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

    public int getPropValList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> rlPropIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductPropDto.Key.LGID, lgId);
            sendBody.putInt(ProductPropDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            rlPropIds.toBuffer(sendBody, ProductPropDto.Key.RL_PROP_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductInfCmd.PropCmd.GET_VAL_LIST);
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
            list.fromBuffer(recvBody, keyRef, ProductPropDto.getPropValInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductPropDto.Key.VAL_LIST) {
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

    public int setPropValList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, FaiList<Param> addList, FaiList<ParamUpdater> setList, FaiList<Integer> delList) {
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
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            sendBody.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropId);
            addList.toBuffer(sendBody, ProductPropDto.Key.VAL_LIST, ProductPropDto.getPropValInfoDto());
            setList.toBuffer(sendBody, ProductPropDto.Key.UPDATERLIST, ProductPropDto.getPropValInfoDto());
            delList.toBuffer(sendBody, ProductPropDto.Key.VAL_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.PropCmd.BATCH_SET_VAL);
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

    public int getPdBindProp(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlLibId, Param propInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            sendBody.putInt(ProductBasicDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductBasicDto.Key.LGID, lgId);
            sendBody.putInt(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            sendBody.putInt(ProductBasicDto.Key.RL_LIB_ID, rlLibId);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductInfCmd.BasicCmd.GET_PROP_LIST);
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
            Ref<ByteBuffer> defBufRef = new Ref<ByteBuffer>();
            m_rt = recvBody.getBuffer(keyRef, defBufRef);
            if(m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.SERIALIZE_TMP_DEF) {
                Log.logErr(m_rt, "recv serialize def codec err");
                return m_rt;
            }
            // 反序列化def
            ParamDef def = Parser.byteBufferToParamDef(defBufRef.value);
            if(def == null) {
                m_rt = Errno.ERROR;
                Log.logErr(m_rt, "deserialize def err;");
                return m_rt;
            }

            m_rt = propInfo.fromBuffer(recvBody, keyRef, def);
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.BIND_PROP_INFO) {
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

    public int setPdBindProp(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> addPropList, FaiList<Param> delPropList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(addPropList == null) {
                addPropList = new FaiList<Param>();
            }
            if(delPropList == null) {
                delPropList = new FaiList<Param>();
            }
            if(addPropList.isEmpty() && delPropList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList and delList all empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            sendBody.putInt(ProductBasicDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductBasicDto.Key.LGID, lgId);
            sendBody.putInt(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            addPropList.toBuffer(sendBody, ProductBasicDto.Key.PROP_BIND, ProductBasicDto.getBindPropValDto());
            delPropList.toBuffer(sendBody, ProductBasicDto.Key.DEL_PROP_BIND, ProductBasicDto.getBindPropValDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.BasicCmd.SET_PROP_LIST);
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

    public int getRlPdByPropVal(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> proIdsAndValIds, FaiList<Integer> rlPdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            rlPdIds.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            sendBody.putInt(ProductBasicDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductBasicDto.Key.LGID, lgId);
            sendBody.putInt(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1);
            proIdsAndValIds.toBuffer(sendBody, ProductBasicDto.Key.BIND_PROP_INFO);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductInfCmd.BasicCmd.GET_RLPDIDS_BY_PROP);
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
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_IDS) {
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
