package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductPropDto;
import fai.MgProductInfSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductInfSvr.interfaces.dto.ProductStoreDto;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreValObj;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

import java.io.IOException;
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductPropDto.getPropInfoDto());
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

            if (idsRef != null) {
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.ERROR;
                    Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                    return m_rt;
                }
                Ref<Integer> keyRef = new Ref<Integer>();
                FaiList<Integer> propIds = new FaiList<Integer>();
                m_rt = propIds.fromBuffer(sendBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductPropDto.Key.RL_PROP_IDS) {
                    Log.logErr(m_rt, "recv rlPropIds codec err");
                    return m_rt;
                }
                idsRef.value = propIds;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    public int setPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<ParamUpdater> updaterList) {
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
        return addPropInfoWithVal(aid, tid, siteId, lgId, keepPriId1, libId, propInfo, propValList, null);
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
            if (propValList == null) {
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
            if (rlPropIdRef != null) {
                FaiBuffer recvBody = recvProtocol.getDecodeBody();
                if (recvBody == null) {
                    m_rt = Errno.ERROR;
                    Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                    return m_rt;
                }
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlPropIdRef);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductPropDto.getPropValInfoDto());
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
            if (addList == null) {
                addList = new FaiList<Param>();
            }
            if (setList == null) {
                setList = new FaiList<ParamUpdater>();
            }
            if (delList == null) {
                delList = new FaiList<Integer>();
            }
            if (addList.isEmpty() && setList.isEmpty() && delList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList, setList, delList all empty;aid=%d;", aid);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductPropDto.Key.TID, tid);
            sendBody.putInt(ProductPropDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductPropDto.Key.LGID, lgId);
            sendBody.putInt(ProductPropDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductPropDto.Key.LIB_ID, libId);
            sendBody.putInt(ProductPropDto.Key.RL_PROP_IDS, rlPropId);
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
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.SERIALIZE_TMP_DEF) {
                Log.logErr(m_rt, "recv serialize def codec err");
                return m_rt;
            }
            // 反序列化def
            ParamDef def = Parser.byteBufferToParamDef(defBufRef.value);
            if (def == null) {
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
            if (addPropList == null) {
                addPropList = new FaiList<Param>();
            }
            if (delPropList == null) {
                delPropList = new FaiList<Param>();
            }
            if (addPropList.isEmpty() && delPropList.isEmpty()) {
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
            proIdsAndValIds.toBuffer(sendBody, ProductBasicDto.Key.BIND_PROP_INFO, ProductBasicDto.getBindPropValDto());
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

    /**
     * 新增商品数据，并添加业务关联
     */
    public int addProductAndRel(int aid, int tid, int siteId, int lgId, int keepPriId1, Param info, Ref<Integer> rlPdIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            if (Str.isEmpty(info)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;info is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            sendBody.putInt(ProductBasicDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductBasicDto.Key.LGID, lgId);
            sendBody.putInt(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1);
            info.toBuffer(sendBody, ProductBasicDto.Key.PD_INFO, ProductBasicDto.getProductDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.BasicCmd.ADD_PD_AND_REL);
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
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_ID) {
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
     * 新增商品业务关联
     */
    public int bindProductRel(int aid, int tid, int siteId, int lgId, int keepPriId1, Param bindRlPdInfo, Param info, Ref<Integer> rlPdIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            if (Str.isEmpty(info)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;info is empty");
                return m_rt;
            }

            if (Str.isEmpty(bindRlPdInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;bindRlPdInfo is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            sendBody.putInt(ProductBasicDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductBasicDto.Key.LGID, lgId);
            sendBody.putInt(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1);
            bindRlPdInfo.toBuffer(sendBody, ProductBasicDto.Key.PD_BIND_INFO, ProductBasicDto.getProductRelDto());
            info.toBuffer(sendBody, ProductBasicDto.Key.PD_REL_INFO, ProductBasicDto.getProductRelDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.BasicCmd.ADD_PD_BIND);
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
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_ID) {
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
     * 批量新增商品业务关联
     */
    public int batchBindProductRel(int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList, Ref<FaiList<Integer>> rlPdIdsRef) {
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
                Log.logErr(m_rt, "args error;infoList is empty");
                return m_rt;
            }

            if (Str.isEmpty(bindRlPdInfo)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;bindRlPdInfo is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            bindRlPdInfo.toBuffer(sendBody, ProductBasicDto.Key.PD_BIND_INFO, ProductBasicDto.getProductRelDto());
            infoList.toBuffer(sendBody, ProductBasicDto.Key.PD_REL_INFO_LIST, ProductBasicDto.getProductRelDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.BasicCmd.BATCH_ADD_PD_BIND);
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

                FaiList<Integer> rlPdIds = new FaiList<Integer>();
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_IDS) {
                    Log.logErr(m_rt, "recv rlPdIds codec err");
                    return m_rt;
                }
                rlPdIdsRef.value = rlPdIds;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 指定业务下，取消 rlPdIds 的商品业务关联
     */
    public int batchDelPdRelBind(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
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
                Log.logErr(m_rt, "args error;rlPdIds is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            sendBody.putInt(ProductBasicDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductBasicDto.Key.LGID, lgId);
            sendBody.putInt(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1);
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.BasicCmd.BATCH_DEL_PD_BIND);
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
     * 删除 rlPdIds 的商品数据及所有商品业务关联数据
     */
    public int batchDelProduct(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) {
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
                Log.logErr(m_rt, "args error;rlPdIds is empty");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductBasicDto.Key.TID, tid);
            sendBody.putInt(ProductBasicDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductBasicDto.Key.LGID, lgId);
            sendBody.putInt(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1);
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.BasicCmd.BATCH_DEL_PDS);
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
     * 批量添加规格模板
     */
    public int addTpScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> list) {
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
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            m_rt = list.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTemp.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SpecTempCmd.ADD_LIST);
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
     * 批量删除规格模板
     */
    public int delTpScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlTpScIdList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (rlTpScIdList == null || rlTpScIdList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            rlTpScIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SpecTempCmd.DEL_LIST);
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
     * 批量修改规格模板
     */
    public int setTpScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<ParamUpdater> updaterList) {
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
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.SpecTemp.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SpecTempCmd.SET_LIST);
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
     * 获取规格模板列表
     */
    public int getTpScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SpecTempCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecTemp.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }


    /**
     * 批量添加规格模板详情
     */
    public int addTpScDetailInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Param> list) {
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
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
            m_rt = list.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTempDetail.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlTpScId=%s;", aid, tid, siteId, lgId, keepPriId1, rlTpScId);
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SpecTempDetailCmd.ADD_LIST);
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
     * 批量删除规格模板详情
     */
    public int delTpScDetailInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Integer> tpScDtIdList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (tpScDtIdList == null || tpScDtIdList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
            tpScDtIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SpecTempDetailCmd.DEL_LIST);
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
     * 批量修改规格模板详情
     */
    public int setTpScDetailInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<ParamUpdater> updaterList) {
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
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
            m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.SpecTempDetail.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlTpScId=%s;", aid, tid, siteId, lgId, keepPriId1, rlTpScId);
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SpecTempDetailCmd.SET_LIST);
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
     * 获取规格模板列表详情
     */
    public int getTpScDetailInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SpecTempDetailCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecTempDetail.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 导入规格模板
     */
    public int importPdScInfo(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlTpScId, FaiList<Integer> tpScDtIdList) {
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
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
            if (tpScDtIdList != null) {
                tpScDtIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.ProductSpecCmd.IMPORT);
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
     * 修改产品规格总接口
     * 批量修改(包括增、删、改)指定商品的商品规格总接口；会自动生成sku规格，并且会调用商品库存服务的“刷新商品库存销售sku”
     */
    public int unionSetPdScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> addList, FaiList<Integer> delList, FaiList<ParamUpdater> updaterList) {
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
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            if (addList != null) {
                m_rt = addList.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.Spec.getInfoDto());
                if(m_rt != Errno.OK){
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "addList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                    return m_rt;
                }
            }
            if (delList != null) {
                delList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            }
            if (updaterList != null) {
                m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.Spec.getInfoDto());
                if(m_rt != Errno.OK){
                    m_rt = Errno.ARGS_ERROR;
                    Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                    return m_rt;
                }
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.ProductSpecCmd.UNION_SET);
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
     * 获取产品规格列表
     */
    public int getPdScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> infoList, boolean onlyGetChecked) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            sendBody.putBoolean(ProductSpecDto.Key.ONLY_GET_CHECKED, onlyGetChecked);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.ProductSpecCmd.GET_LIST);
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
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.Spec.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 批量修改产品规格SKU
     */
    public int setPdSkuScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) {
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
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.SpecSku.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.ProductSpecSkuCmd.SET_LIST);
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
     * 获取产品规格SKU列表
     */
    public int getPdSkuScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductSpecDto.Key.TID, tid);
            sendBody.putInt(ProductSpecDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductSpecDto.Key.LGID, lgId);
            sendBody.putInt(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.ProductSpecSkuCmd.GET_LIST);

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
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 修改商品规格库存销售sku
     */
    public int setPdScSkuSalesStore(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) {
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
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductStoreDto.Key.RL_PD_ID, rlPdId);
            m_rt = updaterList.toBuffer(sendBody, ProductStoreDto.Key.UPDATER_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlPdId=%s;", aid, tid, siteId, lgId, keepPriId1, rlPdId);
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.StoreSalesSkuCmd.SET_LIST);
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
     * 批量扣减库存
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param skuIdCountList    [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode       业务订单id/code
     * @param reduceMode        扣减模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode}
     * @param expireTimeSeconds 扣减模式 - 预扣 下 步骤1 -> 步骤2 过程超时时间，单位s；这个值基本比订单超时时间值大
     */
    public int batchReducePdSkuStore(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode, int expireTimeSeconds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdCountList == null || skuIdCountList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "skuIdCountList error");
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            skuIdCountList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            sendBody.putString(ProductStoreDto.Key.RL_ORDER_CODE, rlOrderCode);
            sendBody.putInt(ProductStoreDto.Key.REDUCE_MODE, reduceMode);
            sendBody.putInt(ProductStoreDto.Key.EXPIRE_TIME_SECONDS, expireTimeSeconds);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_REDUCE_STORE);
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
     * 批量扣除锁住的库存
     * 预扣模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode#HOLDING} 步骤2
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param outStoreRecordInfo 出库记录
     * @return
     */
    public int batchReducePdSkuHoldingStore(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> skuIdCountList, String rlOrderCode, Param outStoreRecordInfo) throws IOException {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdCountList == null || skuIdCountList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg skuIdCountList error");
                return m_rt;
            }
            if(outStoreRecordInfo == null || outStoreRecordInfo.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg outStoreRecordInfo error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            skuIdCountList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            sendBody.putString(ProductStoreDto.Key.RL_ORDER_CODE, rlOrderCode);
            outStoreRecordInfo.toBuffer(sendBody, ProductStoreDto.Key.IN_OUT_STORE_RECODR, ProductStoreDto.InOutStoreRecord.getInfoDto());

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_REDUCE_HOLDING_STORE);
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
     * 批量补偿库存
     * @param skuIdCountList [{ skuId: 122, count:12},{ skuId: 142, count:2}] count > 0
     * @param rlOrderCode 业务订单id/code
     * @param reduceMode
     *  扣减模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode}
     * @return
     */
    public int batchMakeUpPdSkuStore(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> skuIdCountList, String rlOrderCode, int reduceMode) throws IOException {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if(skuIdCountList == null || skuIdCountList.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg skuIdCountList error");
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            skuIdCountList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.StoreSalesSku.getInfoDto());
            sendBody.putString(ProductStoreDto.Key.RL_ORDER_CODE, rlOrderCode);
            sendBody.putInt(ProductStoreDto.Key.REDUCE_MODE, reduceMode);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.StoreSalesSkuCmd.BATCH_MAKE_UP_STORE);
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
     * 获取商品规格库存销售sku
     */
    public int getPdScSkuSalesList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductStoreDto.Key.RL_PD_ID, rlPdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.StoreSalesSkuCmd.GET_LIST);

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
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.StoreSalesSku.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 添加库存出入库记录
     * @param aid
     * @param tid
     * @param siteId 业务商品所属权的 siteId (如:悦客总店的 siteId)
     * @param lgId 业务商品所属权的 lgId (如:悦客总店的 lgId)
     * @param keepPriId1 业务商品所属权的 keepPriId1 (如:悦客总店的 keepPriId1)
     * @param infoList 出入库记录集合，需要包含 siteId, lgId, keepPriId1, skuId
     * @return
     */
    public int addInOutStoreRecordInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            m_rt = infoList.toBuffer(sendBody, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.InOutStoreRecord.getInfoDto());
            if(m_rt != Errno.OK){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
                return m_rt;
            }

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.InOutStoreRecordCmd.ADD_LIST);

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
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }



    /**
     * 获取指定商品所有的业务销售记录 <br/>
     * 适用场景： <br/>
     *    例如：悦客总店查看某个商品时，想进一步查看这个商品在门店的维度下的数据
     */
    public int getPdAllBizSalesSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            sendBody.putInt(ProductStoreDto.Key.RL_PD_ID, rlPdId);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.BizSalesSummaryCmd.GET_ALL_BIZ_LIST_BY_PD_ID);

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
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.BizSalesSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取指定业务下指定商品id集的业务销售信息 <br/>
     * 适用场景： <br/>
     *    例如：悦客门店查看商品 门店维度的信息汇总
     */
    public int getPdBizSalesSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            rlPdIdList.toBuffer(sendBody, ProductStoreDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.BizSalesSummaryCmd.GET_LIST_BY_PD_ID_LIST);

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
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.BizSalesSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }
    /**
     * 获取商品销售记录 <br/>
     * 适用场景： <br/>
     *    例如：悦客总店查看商品信息汇总
     */
    public int getPdSalesSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList, FaiList<Param> infoList){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (infoList == null) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "infoList error");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            rlPdIdList.toBuffer(sendBody, ProductStoreDto.Key.ID_LIST);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.SalesSummaryCmd.GET_LIST);

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
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductStoreDto.SalesSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    /**
     * 获取库存SKU汇总信息
     * 目前分页限制100
     */
    public int getStoreSkuSummaryInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, SearchArg searchArg, FaiList<Param> list){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "aid error");
                return m_rt;
            }
            if(searchArg == null || searchArg.isEmpty()){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "searchArg error");
                return m_rt;
            }
            if(list == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list error");
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductStoreDto.Key.TID, tid);
            sendBody.putInt(ProductStoreDto.Key.SITE_ID, siteId);
            sendBody.putInt(ProductStoreDto.Key.LGID, lgId);
            sendBody.putInt(ProductStoreDto.Key.KEEP_PRIID1, keepPriId1);
            searchArg.toBuffer(sendBody, ProductStoreDto.Key.SEARCH_ARG);

            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setCmd(MgProductInfCmd.StoreSkuSummaryCmd.GET_LIST);
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
            m_rt = list.fromBuffer(recvBody, keyRef, ProductStoreDto.StoreSkuSummary.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductStoreDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if(searchArg.totalSize != null){
                recvBody.getInt(keyRef, searchArg.totalSize);
                if(keyRef.value != ProductStoreDto.Key.TOTAL_SIZE){
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
                    return m_rt;
                }
            }

            return m_rt = Errno.OK;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);
        }
    }

}
