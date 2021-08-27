package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.comm.util.*;

public class MgProductInfCli4ForProductTpSc extends MgProductInfCli3ForProductGroup{
    public MgProductInfCli4ForProductTpSc(int flow) {
        super(flow);
    }

    /**==============================================   商品规格模板接口开始   ==============================================*/
    /**
     * 获取规格模板列表
     *
     * @param infoList Param 见 {@link ProductSpecEntity.SpecTempInfo}
     * @return {@link Errno}
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
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

    /**
     * 获取规格模板列表详情
     *
     * @param rlTpScId 规格模板id {@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID}
     * @param infoList Param 见 {@link ProductSpecEntity.SpecTempDetailInfo} <br/>
     * @return {@link Errno}
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
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

    /**
     * 导入规格模板 到 某个商品
     *
     * @param rlPdId       商品业务id {@link ProductSpecEntity.SpecInfo#RL_PD_ID}
     * @param rlTpScId     规格模板id{@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID}
     * @param tpScDtIdList null(指定规格模板的全部规格详情) || 指定规格模板的部分规格详情Id {@link ProductSpecEntity.SpecTempDetailInfo#TP_SC_DT_ID}
     * @return {@link Errno}
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
            // TODO sysType
            int sysType = 0;

            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.SYS_TYPE, sysType);
            sendBody.putInt(ProductSpecDto.Key.RL_PD_ID, rlPdId);
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
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


    /**
     * 批量添加规格模板
     *
     * @param list Param 见 {@link ProductSpecEntity.SpecTempInfo} <bt/>
     *             {@link ProductSpecEntity.SpecTempInfo#NAME} 必填 <bt/>
     * @return {@link Errno}
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = list.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTemp.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
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

    /**
     * 批量添加规格模板详情
     *
     * @param rlTpScId 规格模板id {@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID}
     * @param list     Param 见 {@link ProductSpecEntity.SpecTempDetailInfo} <br/>
     *                 {@link ProductSpecEntity.SpecTempDetailInfo#NAME} 必填 <br/>
     *                 {@link ProductSpecEntity.SpecTempDetailInfo#IN_SC_VAL_LIST} 必填 <br/>
     * @return {@link Errno}
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
            m_rt = list.toBuffer(sendBody, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTempDetail.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlTpScId=%s;", aid, tid, siteId, lgId, keepPriId1, rlTpScId);
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

    /**
     * 批量修改规格模板
     *
     * @param updaterList Param 见 {@link ProductSpecEntity.SpecTempInfo} <br/>
     *                    {@link ProductSpecEntity.SpecTempInfo#RL_TP_SC_ID} 必须要有
     * @return {@link Errno}
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.SpecTemp.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;", aid, tid, siteId, lgId, keepPriId1);
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


    /**
     * 批量修改规格模板详情
     *
     * @param rlTpScId    {@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID} 规格模板id
     * @param updaterList Param 见 {@link ProductSpecEntity.SpecTempDetailInfo} <br/>
     *                    {@link ProductSpecEntity.SpecTempDetailInfo#TP_SC_DT_ID} 必填 <br/>
     * @return {@link Errno}
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
            m_rt = updaterList.toBuffer(sendBody, ProductSpecDto.Key.UPDATER_LIST, ProductSpecDto.SpecTempDetail.getInfoDto());
            if (m_rt != Errno.OK) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList err;aid=%s;tid=%s;siteId=%s;lgId=%s;keepPriId1=%s;rlTpScId=%s;", aid, tid, siteId, lgId, keepPriId1, rlTpScId);
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


    /**
     * 批量删除规格模板
     *
     * @param rlTpScIdList 规格模板id集合
     * @return {@link Errno}
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            rlTpScIdList.toBuffer(sendBody, ProductSpecDto.Key.ID_LIST);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.SpecTempCmd.DEL_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量删除规格模板详情
     *
     * @param rlTpScId     规格模板id {@link ProductSpecEntity.SpecTempDetailInfo#RL_TP_SC_ID
     * @param tpScDtIdList 集合中的元素为 规格详情id {@link ProductSpecEntity.SpecTempDetailInfo#TP_SC_DT_ID}
     * @return {@link Errno}
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
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductSpecDto.Key.TID, tid), new Pair(ProductSpecDto.Key.SITE_ID, siteId), new Pair(ProductSpecDto.Key.LGID, lgId), new Pair(ProductSpecDto.Key.KEEP_PRIID1, keepPriId1));
            sendBody.putInt(ProductSpecDto.Key.RL_TP_SC_ID, rlTpScId);
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
