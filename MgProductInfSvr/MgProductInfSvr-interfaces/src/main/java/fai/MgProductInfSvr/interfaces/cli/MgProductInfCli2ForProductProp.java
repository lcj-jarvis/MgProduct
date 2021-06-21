package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductPropDto;
import fai.MgProductInfSvr.interfaces.entity.ProductPropEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.comm.util.*;

import java.nio.ByteBuffer;

public class MgProductInfCli2ForProductProp extends MgProductInfCli1ForProductBasic {
    public MgProductInfCli2ForProductProp(int flow) {
        super(flow);
    }

    @Deprecated
    public int getPdBindProp(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlLibId, Param propInfo) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdId(rlPdId)
                .setRlLibId(rlLibId)
                .build();
        return getPdBindProp(mgProductArg, propInfo);
    }

    @Deprecated
    public int getRlPdByPropVal(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> propIdsAndValIds, FaiList<Integer> rlPdIds) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setPropIdsAndValIds(propIdsAndValIds)
                .build();
        return getRlPdByPropVal(mgProductArg, rlPdIds);
    }

    @Deprecated
    public int getPropValList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> rlPropIds, FaiList<Param> list) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlLibId(libId)
                .setRlPropIds(rlPropIds)
                .build();
        return getPropValList(mgProductArg, list);
    }

    @Deprecated
    public int getPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, SearchArg searchArg, FaiList<Param> list) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlLibId(libId)
                .setSearchArg(searchArg)
                .build();
        return getPropList(mgProductArg, list);
    }

    @Deprecated
    public int addPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Param> list, Ref<FaiList<Integer>> idsRef) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlLibId(libId)
                .setAddList(list)
                .build();
        return addPropList(mgProductArg, idsRef);
    }

    @Deprecated
    public int setPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<ParamUpdater> updaterList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlLibId(libId)
                .setUpdaterList(updaterList)
                .build();
        return setPropList(mgProductArg);
    }

    @Deprecated
    public int delPropList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> idList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlLibId(libId)
                .setRlPropIds(idList)
                .build();
        return delPropList(mgProductArg);
    }

    @Deprecated
    public int addPropInfoWithVal(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, Param propInfo, FaiList<Param> propValList) {
        return addPropInfoWithVal(aid, tid, siteId, lgId, keepPriId1, libId, propInfo, propValList, null);
    }

    @Deprecated
    public int addPropInfoWithVal(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, Param propInfo, FaiList<Param> propValList, Ref<Integer> rlPropIdRef) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlLibId(libId)
                .setAddInfo(propInfo)
                .setAddValList(propValList)
                .build();
        return addPropInfoWithVal(mgProductArg, rlPropIdRef);
    }

    @Deprecated
    public int setPropAndVal(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, ParamUpdater propUpdater, FaiList<Param> addValList, FaiList<ParamUpdater> setValUpList, FaiList<Integer> delValIds) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlLibId(libId)
                .setRlPropId(rlPropId)
                .setUpdater(propUpdater)
                .setAddValList(addValList)
                .setSetValList(setValUpList)
                .setDelValList(delValIds)
                .build();
        return setPropAndVal(mgProductArg);
    }

    @Deprecated
    public int setPropValList(int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, FaiList<Param> addList, FaiList<ParamUpdater> setList, FaiList<Integer> delList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlLibId(libId)
                .setRlPropId(rlPropId)
                .setAddList(addList)
                .setUpdaterList(setList)
                .setDelValList(delList)
                .build();
        return setPropValList(mgProductArg);
    }

    @Deprecated
    public int setPdBindProp(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> addPropList, FaiList<Param> delPropList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdId(rlPdId)
                .setAddList(addPropList)
                .setPropIdsAndValIds(delPropList)
                .build();
        return setPdBindProp(mgProductArg);
    }

    /**==============================================   商品参数接口开始   ==============================================*/
    /**
     * 根据 libId、商品 rlPdId, 获取绑定的参数信息
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdId(rlPdId)    // 必填
     *                 .setRlLibId(rlLibId)  // 必填
     *                 .build();
     * @param propInfo 接收绑定的参数信息
     * @return {@link Errno}
     */
    public int getPdBindProp(MgProductArg mgProductArg, Param propInfo) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int rlLibId = mgProductArg.getRlLibId();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductBasicDto.Key.RL_LIB_ID, rlLibId));
            int rlPdId = mgProductArg.getRlPdId();
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            // send and recv
            int aid = mgProductArg.getAid();
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

    /**
     * 根据商品参数id和商品参数值id，返回商品业务id集合
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setPropIdsAndValIds(propIdsAndValIds)  // 必填
     *                 .build();
     * @param rlPdIds 商品业务id集合
     * @return {@link Errno}
     */
    public int getRlPdByPropVal(MgProductArg mgProductArg, FaiList<Integer> rlPdIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            rlPdIds.clear();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            FaiList<Param> propIdsAndValIds = mgProductArg.getPropIdsAndValIds();
            propIdsAndValIds.toBuffer(sendBody, ProductBasicDto.Key.BIND_PROP_INFO, ProductBasicDto.getBindPropValDto());
            // send and recv
            int aid = mgProductArg.getAid();
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

    /**
     * 根据 libId、参数 rlPropIds, 获取参数值list
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlLibId(libId)        // 必填
     *                 .setRlPropIds(rlPropIds)  // 必填
     *                 .build();
     * @param list 参数值 list
     * @return {@link Errno}
     */
    public int getPropValList(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int libId = mgProductArg.getRlLibId();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            FaiList<Integer> rlPropIds = mgProductArg.getRlPropIds();
            rlPropIds.toBuffer(sendBody, ProductPropDto.Key.RL_PROP_IDS);
            // send and recv
            int aid = mgProductArg.getAid();
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

    /**
     * 根据 libId、searchArg，搜索 参数 列表
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlLibId(libId)        // 必填
     *                 .setSearchArg(searchArg)  // 选填
     *                 .build();
     * @param list 接收参数列表
     * @return {@link Errno}
     */
    public int getPropList(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int libId = mgProductArg.getRlLibId();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            SearchArg searchArg = mgProductArg.getSearchArg();
            if (searchArg == null) {
                searchArg = new SearchArg();
            }
            searchArg.toBuffer(sendBody, ProductPropDto.Key.SEARCH_ARG);
            int aid = mgProductArg.getAid();
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

    /**
     * 根据 libId、list，批量 增加参数，返回对应 新增参数对应的 idsRef
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlLibId(libId)   // 必填
     *                 .setAddList(list)    // 必填
     *                 .build();
     * @param idsRef 新增参数对应的 idsRef
     * @return {@link Errno}
     */
    public int addPropList(MgProductArg mgProductArg, Ref<FaiList<Integer>> idsRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> list = mgProductArg.getAddList();
            if (list == null || list.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int libId = mgProductArg.getRlLibId();
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

    /**
     * 根据 libId、updaterList， 批量 修改参数
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlLibId(libId)            // 必填
     *                 .setUpdaterList(updaterList)  // 必填
     *                 .build();
     * @return {@link Errno}
     */
    public int setPropList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<ParamUpdater> updaterList = mgProductArg.getUpdaterList();
            if (updaterList == null || updaterList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int libId = mgProductArg.getRlLibId();
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

    /**
     * 根据 libId、idList， 批量 删除参数
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlLibId(libId)      // 必填
     *                 .setRlPropIds(idList)   // 必填
     *                 .build();
     * @return {@link Errno}
     */
    public int delPropList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> idList = mgProductArg.getRlPropIds();
            if (idList == null || idList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int libId = mgProductArg.getRlLibId();
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
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *              .setAddList(addList)         // 选填
     *              .setUpdaterList(updaterList) // 选填
     *              .setRlPropIds(rlPropIds)     // 选填
     *              .setRlLibId(libId)           // 必填
     *              .build();
     * addList: 详见{@link ProductPropEntity.PropInfo}
     * updaterList: 详见{@link ProductPropEntity.PropInfo}
     * rlPropIds: 参数业务id集合
     * libId: 库id
     * @param idsRef 接收返回的rlPropIds
     * @return {@link Errno}
     */
    public int unionSetPropList(MgProductArg mgProductArg, Ref<FaiList<Integer>> idsRef) {
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
            int libId = mgProductArg.getRlLibId();
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            FaiList<Param> addList = mgProductArg.getAddList();
            if (addList == null) {
                addList = new FaiList<Param>();
            }
            m_rt = addList.toBuffer(sendBody, ProductPropDto.Key.ADD_LIST, ProductPropDto.getPropInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "addList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;libId=%s;", aid, tid, siteId, lgId, keepPriId1, libId);
                return m_rt;
            }
            FaiList<ParamUpdater> updaterList = mgProductArg.getUpdaterList();
            if (updaterList == null) {
                updaterList = new FaiList<ParamUpdater>();
            }
            m_rt = updaterList.toBuffer(sendBody, ProductPropDto.Key.UPDATER_LIST, ProductPropDto.getPropInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;libId=%s;", aid, tid, siteId, lgId, keepPriId1, libId);
                return m_rt;
            }
            FaiList<Integer> delList = mgProductArg.getRlPropIds();
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

    /**
     * 根据 libId、propInfo、propValList， 增加 参数 以及 批量增加这个参数 所有的参数值
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *             .setRlLibId(libId)           // 必填
     *             .setAddInfo(propInfo)        // 必填
     *             .setAddValList(propValList)  // 选填
     *             .build();
     * @param rlPropIdRef 参数业务id
     * @return {@link Errno}
     */
    public int addPropInfoWithVal(MgProductArg mgProductArg, Ref<Integer> rlPropIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            Param propInfo = mgProductArg.getAddInfo();
            if (propInfo == null) {
                propInfo = new Param();
            }
            FaiList<Param> propValList = mgProductArg.getAddValList();
            if (propValList == null) {
                propValList = new FaiList<Param>();
            }
            if (propInfo.isEmpty() && propValList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "propInfo, propValList all null");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int libId = mgProductArg.getRlLibId();
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

    /**
     * 根据 libId、rlPropId、propUpdater、addList、setValUpList、delValIds，修改 参数 以及 批量 修改、增加、删除 单个参数 的 参数值
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlLibId(libId)            // 必填
     *                 .setRlPropId(rlPropId)        // 必填
     *                 .setUpdater(propUpdater)      // 选填
     *                 .setAddValList(addValList)    // 选填
     *                 .setSetValList(setValUpList)  // 选填
     *                 .setDelValList(delValIds)     // 选填
     *                 .build();
     * @return {@link Errno}
     */
    public int setPropAndVal(MgProductArg mgProductArg) {
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
            int libId = mgProductArg.getRlLibId();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            int rlPropId = mgProductArg.getRlPropId();
            sendBody.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropId);
            ParamUpdater propUpdater = mgProductArg.getUpdater();
            if (propUpdater == null) {
                propUpdater = new ParamUpdater();
            }
            FaiList<Param> addValList = mgProductArg.getAddValList();
            if (addValList == null) {
                addValList = new FaiList<Param>();
            }
            FaiList<ParamUpdater> setValUpList = mgProductArg.getSetValList();
            if (setValUpList == null) {
                setValUpList = new FaiList<ParamUpdater>();
            }
            FaiList<Integer> delValIds = mgProductArg.getDelValList();
            if (delValIds == null) {
                delValIds = new FaiList<Integer>();
            }
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

    /**
     * 根据 libId、rlPropId、addList、setList、delList，批量 修改、增加、删除 单个参数 的 参数值
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlLibId(libId)       // 必填
     *                 .setRlPropId(rlPropId)   // 必填
     *                 .setAddList(addList)     // 选填
     *                 .setUpdaterList(setList) // 选填
     *                 .setDelValList(delList)  // 选填
     *                 .build();
     * @return {@link Errno}
     */
    public int setPropValList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> addList = mgProductArg.getAddList();
            if (addList == null) {
                addList = new FaiList<Param>();
            }
            FaiList<ParamUpdater> setList = mgProductArg.getUpdaterList();
            if (setList == null) {
                setList = new FaiList<ParamUpdater>();
            }
            FaiList<Integer> delList = mgProductArg.getRlPropIds();
            if (delList == null) {
                delList = new FaiList<Integer>();
            }
            if (addList.isEmpty() && setList.isEmpty() && delList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList, setList, delList all empty;aid=%d;", aid);
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            int libId = mgProductArg.getRlLibId();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductPropDto.Key.TID, tid), new Pair(ProductPropDto.Key.SITE_ID, siteId), new Pair(ProductPropDto.Key.LGID, lgId), new Pair(ProductPropDto.Key.KEEP_PRIID1, keepPriId1), new Pair(ProductPropDto.Key.LIB_ID, libId));
            int rlPropId = mgProductArg.getRlPropId();
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

    /**
     * 根据商品 rlPdId、addPropList、delPropList, 绑定参数、删除绑定的参数
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdId(rlPdId)                // 必填
     *                 .setAddList(addPropList)          // 选填
     *                 .setPropIdsAndValIds(delPropList) // 选填
     *                 .build();
     * @return {@link Errno}
     */
    public int setPdBindProp(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Param> addPropList = mgProductArg.getAddList();
            if (addPropList == null) {
                addPropList = new FaiList<Param>();
            }
            FaiList<Param> delPropList = mgProductArg.getPropIdsAndValIds();
            if (delPropList == null) {
                delPropList = new FaiList<Param>();
            }
            if (addPropList.isEmpty() && delPropList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList and delList all empty");
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            int rlPdId = mgProductArg.getRlPdId();
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
