package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductPropDto;
import fai.comm.util.*;

import java.nio.ByteBuffer;

public class MgProductInfCli2ForProductProp extends MgProductInfCli1ForProductBasic {
    public MgProductInfCli2ForProductProp(int flow) {
        super(flow);
    }

    /**==============================================   商品参数接口开始   ==============================================*/
    // 根据 libId、商品 rlPdId, 获取绑定的参数信息
    public int getPdBindProp(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlLibId, Param propInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductBasicDto.Key.RL_LIB_ID, rlLibId));
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.GET_PROP_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
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
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    // 根据商品 rlPdIds, 获取绑定的 参数信息 以及 参数值 list
    public int getRlPdByPropVal(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> proIdsAndValIds, FaiList<Integer> rlPdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            rlPdIds.clear();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            proIdsAndValIds.toBuffer(sendBody, ProductBasicDto.Key.BIND_PROP_INFO, ProductBasicDto.getBindPropValDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.GET_RLPDIDS_BY_PROP, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = rlPdIds.fromBuffer(recvBody, keyRef);
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.RL_PD_IDS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    // 根据 libId、参数 rlPropIds, 获取参数值list
    public int getPropValList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> rlPropIds, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            rlPropIds.toBuffer(sendBody, ProductPropDto.Key.RL_PROP_IDS);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.GET_VAL_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductPropDto.getPropValInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductPropDto.Key.VAL_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    // 根据 libId、searchArg，搜索 参数 列表
    public int getPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, SearchArg searchArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            searchArg.toBuffer(sendBody, ProductPropDto.Key.SEARCH_ARG);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
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
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    // 根据 libId、list，批量 增加参数，返回对应 新增参数对应的 idsRef
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            list.toBuffer(sendBody, ProductPropDto.Key.PROP_LIST, ProductPropDto.getPropInfoDto());
            // send and recv
            boolean idsRefNotNull = (idsRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.BATCH_ADD, sendBody, false, idsRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (idsRefNotNull) {
                Ref<Integer> keyRef = new Ref<Integer>();
                FaiList<Integer> propIds = new FaiList<Integer>();
                m_rt = propIds.fromBuffer(recvBody, keyRef);
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

    // 根据 libId、updaterList， 批量 修改参数
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            updaterList.toBuffer(sendBody, ProductPropDto.Key.UPDATERLIST, ProductPropDto.getPropInfoDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.BATCH_SET, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    // 根据 libId、idList， 批量 删除参数
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            idList.toBuffer(sendBody, ProductPropDto.Key.RL_PROP_IDS);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.BATCH_DEL, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 合并 商品参数 增删改 接口
     * @param addList 添加数据
     * @param updaterList 修改数据
     * @param delList 删除数据
     * @param idsRef 添加成功后返回的id
     * @return rt
     */
    public int unionSetPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList, Ref<FaiList<Integer>> idsRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            if (addList == null) {
                addList = new FaiList<Param>();
            }
            m_rt = addList.toBuffer(sendBody, ProductPropDto.Key.ADD_LIST, ProductPropDto.getPropInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "addList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;libId=%s;", aid, tid, siteId, lgId, keepPriId1, libId);
                return m_rt;
            }
            if (updaterList == null) {
                updaterList = new FaiList<ParamUpdater>();
            }
            m_rt = updaterList.toBuffer(sendBody, ProductPropDto.Key.UPDATER_LIST, ProductPropDto.getPropInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;libId=%s;", aid, tid, siteId, lgId, keepPriId1, libId);
                return m_rt;
            }
            if (delList == null) {
                delList = new FaiList<Integer>();
            }
            m_rt = delList.toBuffer(sendBody, ProductPropDto.Key.DEL_LIST);
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "delList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;libId=%s;", aid, tid, siteId, lgId, keepPriId1, libId);
                return m_rt;
            }
            boolean idsRefNotNull = (idsRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.UNION_SET_PROP_LIST, sendBody, false, idsRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (idsRefNotNull) {
                Ref<Integer> keyRef = new Ref<Integer>();
                FaiList<Integer> propIds = new FaiList<Integer>();
                m_rt = propIds.fromBuffer(recvBody, keyRef);
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

    // 根据 libId、propInfo、propValList， 增加 参数 以及 批量增加这个参数 所有的参数值
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            propInfo.toBuffer(sendBody, ProductPropDto.Key.PROP_INFO, ProductPropDto.getPropInfoDto());
            propValList.toBuffer(sendBody, ProductPropDto.Key.VAL_LIST, ProductPropDto.getPropValInfoDto());
            // send and recv
            boolean rlPropIdRefNotNull = (rlPropIdRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.ADD_WITH_VAL, sendBody, false, rlPropIdRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (rlPropIdRef != null) {
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

    // 根据 libId、rlPropId、propUpdater、addList、setValUpList、delValIds，修改 参数 以及 批量 修改、增加、删除 单个参数 的 参数值
    public int setPropAndVal(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, ParamUpdater propUpdater, FaiList<Param> addValList, FaiList<ParamUpdater> setValUpList, FaiList<Integer> delValIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            sendBody.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropId);
            propUpdater.toBuffer(sendBody, ProductPropDto.Key.UPDATER, ProductPropDto.getPropInfoDto());
            addValList.toBuffer(sendBody, ProductPropDto.Key.VAL_LIST, ProductPropDto.getPropValInfoDto());
            setValUpList.toBuffer(sendBody, ProductPropDto.Key.UPDATERLIST, ProductPropDto.getPropValInfoDto());
            delValIds.toBuffer(sendBody, ProductPropDto.Key.VAL_IDS);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.SET_WITH_VAL, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    // 根据 libId、rlPropId、addList、setList、delList，批量 修改、增加、删除 单个参数 的 参数值
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            sendBody.putInt(ProductPropDto.Key.RL_PROP_IDS, rlPropId);
            addList.toBuffer(sendBody, ProductPropDto.Key.VAL_LIST, ProductPropDto.getPropValInfoDto());
            setList.toBuffer(sendBody, ProductPropDto.Key.UPDATERLIST, ProductPropDto.getPropValInfoDto());
            delList.toBuffer(sendBody, ProductPropDto.Key.VAL_IDS);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.PropCmd.BATCH_SET_VAL, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    // 根据商品 rlPdId、addPropList、delPropList, 绑定参数、删除绑定的参数
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            addPropList.toBuffer(sendBody, ProductBasicDto.Key.PROP_BIND, ProductBasicDto.getBindPropValDto());
            delPropList.toBuffer(sendBody, ProductBasicDto.Key.DEL_PROP_BIND, ProductBasicDto.getBindPropValDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.SET_PROP_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    /**----------------------------------------------   商品参数接口结束   ----------------------------------------------*/
}
