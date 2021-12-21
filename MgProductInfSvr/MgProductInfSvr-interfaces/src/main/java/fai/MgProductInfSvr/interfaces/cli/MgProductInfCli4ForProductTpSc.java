package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.comm.util.*;

public class MgProductInfCli4ForProductTpSc extends MgProductInfCli3ForProductGroup{
    public MgProductInfCli4ForProductTpSc(int flow) {
        super(flow);
    }

    /**==============================================   商品规格模板接口开始   ==============================================*/
    @Deprecated
    public int getTpScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> infoList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .build();
        return getTpScInfoList(mgProductArg, infoList);
    }
    /**
     * 获取规格模板列表
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .build();
     * @param infoList Param 见 {@link ProductSpecEntity.SpecTempInfo}
     * @return {@link Errno}
     */
    public int getTpScInfoList(MgProductArg mgProductArg, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
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
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecTemp.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    @Deprecated
    public int getTpScDetailInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Param> infoList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlTpScId(rlTpScId)
                .build();
        return getTpScDetailInfoList(mgProductArg, infoList);
    }
    /**
     * 获取规格模板列表详情
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlTpScId(rlTpScId) //规格模板id {@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID}
     *                 .build();
     * @param infoList Param 见 {@link ProductSpecEntity.SpecTempDetailInfo} <br/>
     * @return {@link Errno}
     */
    public int getTpScDetailInfoList(MgProductArg mgProductArg, FaiList<Param> infoList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
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
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, mgProductArg.getRlTpScId());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempDetailCmd.GET_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = infoList.fromBuffer(recvBody, keyRef, ProductSpecDto.SpecTempDetail.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductSpecDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }

    @Deprecated
    public int importPdScInfo(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlTpScId, FaiList<Integer> tpScDtIdList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdId(rlPdId)
                .setRlTpScId(rlTpScId)
                .setTpScDtIds(tpScDtIdList)
                .build();
        return importPdScInfo(mgProductArg);
    }
    /**
     * 导入规格模板 到 某个商品
     *
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *     .setRlPdId(rlPdId) // 商品业务id {@link ProductSpecEntity.SpecInfo#RL_PD_ID}
     *     .setRlTpScId(rlTpScId) // 规格模板id{@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID}
     *     .setTpScDtIds(tpScDtIdList) // null(指定规格模板的全部规格详情) || 指定规格模板的部分规格详情Id {@link ProductSpecEntity.SpecTempDetailInfo#TP_SC_DT_ID}
     *     .build();
     * @return {@link Errno}
     */
    public int importPdScInfo(MgProductArg mgProductArg) {
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
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, mgProductArg.getRlPdId());
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, mgProductArg.getRlTpScId());
            FaiList<Integer> tpScDtIdList = mgProductArg.getTpScDtIds();
            if (tpScDtIdList != null) {
                tpScDtIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.ProductSpecCmd.IMPORT, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    @Deprecated
    public int addTpScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> list) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setAddList(list) // 要添加的数据集合
                .build();
        return addTpScInfoList(mgProductArg);
    }

    /**
     * 批量添加规格模板
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *     .setAddList(list) // 要添加的数据集合 Param 见 {@link ProductSpecEntity.SpecTempInfo} <bt/> {@link ProductSpecEntity.SpecTempInfo#NAME} 必填 <bt/>
     *     .build();
     * @return
     */
    public int addTpScInfoList(MgProductArg mgProductArg) {
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
            // packaging send data
            FaiBuffer sendBody = getPrimaryKeyBuffer(mgProductArg);
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            m_rt = list.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTemp.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list err;aid=%s;mgProductArg=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, mgProductArg);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempCmd.ADD_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    @Deprecated
    public int addTpScDetailInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Param> list) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlTpScId(rlTpScId)
                .setAddList(list)
                .build();
        return addTpScDetailInfoList(mgProductArg);
    }
    /**
     * 批量添加规格模板详情
     *
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlTpScId(rlTpScId) // 规格模板id {@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID}
     *                 .setAddList(list)
     *                 .build();
     * list说明:     Param 见 {@link ProductSpecEntity.SpecTempDetailInfo} <br/>
     *                 {@link ProductSpecEntity.SpecTempDetailInfo#NAME} 必填 <br/>
     *                 {@link ProductSpecEntity.SpecTempDetailInfo#IN_SC_VAL_LIST} 必填 <br/>
     * @return {@link Errno}
     */
    public int addTpScDetailInfoList(MgProductArg mgProductArg) {
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
            // packaging send data
            FaiBuffer sendBody = getPrimaryKeyBuffer(mgProductArg);
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, mgProductArg.getRlTpScId());
            m_rt = list.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTempDetail.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list err;aid=%s;mgProductArg=%s;", aid, mgProductArg);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempDetailCmd.ADD_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    @Deprecated
    public int setTpScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<ParamUpdater> updaterList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setUpdaterList(updaterList)
                .build();
        return setTpScInfoList(mgProductArg);
    }
    /**
     * 批量修改规格模板
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setUpdaterList(updaterList)
     *                 .build();
     * updaterList说明： Param 见 {@link ProductSpecEntity.SpecTempInfo} <br/>
     *                    {@link ProductSpecEntity.SpecTempInfo#RL_TP_SC_ID} 必须要有
     * @return {@link Errno}
     */
    public int setTpScInfoList(MgProductArg mgProductArg) {
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
            // packaging send data
            FaiBuffer sendBody = getPrimaryKeyBuffer(mgProductArg);
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.SpecTemp.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;mgProductArg=%s;", aid, mgProductArg);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempCmd.SET_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }


    @Deprecated
    public int setTpScDetailInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<ParamUpdater> updaterList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlTpScId(rlTpScId)
                .setUpdaterList(updaterList)
                .build();
        return setTpScDetailInfoList(mgProductArg);
    }
    /**
     * 批量修改规格模板详情
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlTpScId(rlTpScId)
     *                 .setUpdaterList(updaterList)
     *                 .build();
     * rlTpScId:    {@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID} 规格模板id
     * updaterList: Param 见 {@link ProductSpecEntity.SpecTempDetailInfo} <br/>
     *                    {@link ProductSpecEntity.SpecTempDetailInfo#TP_SC_DT_ID} 必填 <br/>
     * @return {@link Errno}
     */
    public int setTpScDetailInfoList(MgProductArg mgProductArg) {
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
            // packaging send data
            FaiBuffer sendBody = getPrimaryKeyBuffer(mgProductArg);
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, mgProductArg.getRlTpScId());
            m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.SpecTempDetail.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;mgProductArg=%s;", aid, mgProductArg);
                return m_rt;
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempDetailCmd.SET_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }


    @Deprecated
    public int delTpScInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlTpScIdList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlTpScIds(rlTpScIdList)
                .build();
        return delTpScInfoList(mgProductArg);
    }
    /**
     * 批量删除规格模板
     *
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlTpScIds(rlTpScIdList)
     *                 .build();
     * rlTpScIdList: 规格模板id集合
     * @return {@link Errno}
     */
    public int delTpScInfoList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> rlTpScIdList = mgProductArg.getRlTpScIds();
            if (rlTpScIdList == null || rlTpScIdList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getPrimaryKeyBuffer(mgProductArg);
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            rlTpScIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempCmd.DEL_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    @Deprecated
    public int delTpScDetailInfoList(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Integer> tpScDtIdList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlTpScId(rlTpScId)
                .setTpScDtIds(tpScDtIdList)
                .build();
        return delTpScDetailInfoList(mgProductArg);
    }
    /**
     * 批量删除规格模板详情
     *
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlTpScId(rlTpScId)
     *                 .setTpScDtIds(tpScDtIdList)
     *                 .build();
     * rlTpScId: 规格模板id {@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID
     * tpScDtIdList: 集合中的元素为 规格详情id {@link ProductSpecEntity.SpecTempDetailInfo#TP_SC_DT_ID}
     * @return {@link Errno}
     */
    public int delTpScDetailInfoList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> tpScDtIdList = mgProductArg.getTpScDtIds();
            if (tpScDtIdList == null || tpScDtIdList.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getPrimaryKeyBuffer(mgProductArg);
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, mgProductArg.getSysType());
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, mgProductArg.getRlTpScId());
            tpScDtIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempDetailCmd.DEL_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    /**----------------------------------------------   商品规格模板接口结束   ----------------------------------------------*/
}
