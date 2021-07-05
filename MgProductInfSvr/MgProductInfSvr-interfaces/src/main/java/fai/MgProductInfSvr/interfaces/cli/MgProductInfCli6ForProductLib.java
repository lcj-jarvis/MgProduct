package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductGroupDto;
import fai.MgProductInfSvr.interfaces.dto.ProductLibDto;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;

/**
 * @author LuChaoJi
 * @date 2021-07-01 10:45
 */
public class MgProductInfCli6ForProductLib extends MgProductInfCli5ForProductScAndStore {
    public MgProductInfCli6ForProductLib(int flow) {
        super(flow);
    }
    
    /**==============================================   商品库服务接口开始   ==============================================*/
    /**
     * 新增商品库
     */
    public int addProductLib(int aid, int tid, int siteId, int lgId, int keepPriId1,
                             Param info, Ref<Integer> libIdRef, Ref<Integer> rlLibIdRef) {
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
                Log.logErr(m_rt, "info is null;aid=%d;", aid);
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductLibDto.Key.TID, tid),
                    new Pair(ProductLibDto.Key.SITE_ID, siteId),
                    new Pair(ProductLibDto.Key.LGID, lgId),
                    new Pair(ProductLibDto.Key.KEEP_PRIID1, keepPriId1));
            info.toBuffer(sendBody, ProductLibDto.Key.INFO, ProductLibDto.getPdLibDto());

            // send and recv
            boolean needReceive = (libIdRef != null) || (rlLibIdRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.LibCmd.ADD_LIB, sendBody,
                    false, needReceive);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            Ref<Integer> keyRef = new Ref<Integer>();
            if (libIdRef != null) {
                m_rt = recvBody.getInt(keyRef, rlLibIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductLibDto.Key.LIB_ID) {
                    Log.logErr(m_rt, "recv libId codec err");
                    return m_rt;
                }
            }
            if (rlLibIdRef != null) {
                m_rt = recvBody.getInt(keyRef, libIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductLibDto.Key.RL_LIB_ID) {
                    Log.logErr(m_rt, "recv rlLibId codec err");
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
     * 删除商品库
     * @param rlLibIds 要删除的商品库业务id集合
     * @return
     */
    public int delPdLibList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlLibIds) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            if (Util.isEmptyList(rlLibIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlLibIds is null;aid=%d;", aid);
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductLibDto.Key.TID, tid),
                    new Pair(ProductLibDto.Key.SITE_ID, siteId),
                    new Pair(ProductLibDto.Key.LGID, lgId),
                    new Pair(ProductLibDto.Key.KEEP_PRIID1, keepPriId1));
            rlLibIds.toBuffer(sendBody, ProductLibDto.Key.RL_LIB_IDS);

            // send and recv
            sendAndRecv(aid, MgProductInfCmd.LibCmd.DEL_LIB_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 修改商品库
     * @param updaterList 修改信息
     * @return
     */
    public int setPdLibList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<ParamUpdater> updaterList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (Util.isEmptyList(updaterList)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "updaterList is null;aid=%d;", aid);
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductLibDto.Key.TID, tid),
                    new Pair(ProductLibDto.Key.SITE_ID, siteId),
                    new Pair(ProductLibDto.Key.LGID, lgId),
                    new Pair(ProductLibDto.Key.KEEP_PRIID1, keepPriId1));
            updaterList.toBuffer(sendBody, ProductLibDto.Key.UPDATERLIST, ProductLibDto.getPdLibDto());
            // send and recv
            sendAndRecv(aid, MgProductInfCmd.LibCmd.SET_LIB_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 查询商品库的数据
     */
    public int getPdLibList(int aid, int tid, int siteId, int lgId, int keepPriId1,
                            SearchArg searchArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            list.clear();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductLibDto.Key.TID, tid),
                    new Pair(ProductLibDto.Key.SITE_ID, siteId),
                    new Pair(ProductLibDto.Key.LGID, lgId),
                    new Pair(ProductLibDto.Key.KEEP_PRIID1, keepPriId1));
            searchArg.toBuffer(sendBody, ProductLibDto.Key.SEARCH_ARG);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.LibCmd.GET_LIB_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductLibDto.getPdLibDto());
            if (m_rt != Errno.OK || keyRef.value != ProductLibDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 联合增删改。先删除，再修改，最后添加
     * @param addInfoList 要添加的
     * @param updaterList 要更新的
     * @param delRlLibIds 要删除的
     * @param rlLibIdsRef 保存新增商品的rlLibId
     * @return
     */
    public int unionSetLibList(int aid, int tid, int siteId, int lgId, int keepPriId1,
                                 FaiList<Param> addInfoList,
                                 FaiList<ParamUpdater> updaterList,
                                 FaiList<Integer> delRlLibIds,
                                 FaiList<Integer> rlLibIdsRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductLibDto.Key.TID, tid),
                    new Pair(ProductLibDto.Key.SITE_ID, siteId),
                    new Pair(ProductLibDto.Key.LGID, lgId),
                    new Pair(ProductLibDto.Key.KEEP_PRIID1, keepPriId1));

            if (Util.isEmptyList(addInfoList)) {
                addInfoList = new FaiList<Param>();
            }
            addInfoList.toBuffer(sendBody, ProductLibDto.Key.INFO_LIST, ProductLibDto.getPdLibDto());
            if (updaterList == null) {
                updaterList = new FaiList<ParamUpdater>();
            }
            updaterList.toBuffer(sendBody, ProductLibDto.Key.UPDATERLIST, ProductLibDto.getPdLibDto());
            if (delRlLibIds == null) {
                delRlLibIds = new FaiList<Integer>();
            }
            delRlLibIds.toBuffer(sendBody, ProductLibDto.Key.RL_LIB_IDS);

            boolean needRlLibIds = (rlLibIdsRef != null);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.LibCmd.UNION_SET_LIB_LIST, sendBody,
                    false, needRlLibIds);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            if (!Util.isEmptyList(addInfoList)) {
                Ref<Integer> keyRef = new Ref<Integer>();
                rlLibIdsRef.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductLibDto.Key.RL_LIB_IDS) {
                    Log.logErr(m_rt, "recv rlLibIds codec err");
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
     * 获取所有的库业务表的数据
     */
    public int getAllLibRel(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> relLibList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            relLibList.clear();

            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductLibDto.Key.TID, tid),
                    new Pair(ProductLibDto.Key.SITE_ID, siteId),
                    new Pair(ProductLibDto.Key.LGID, lgId),
                    new Pair(ProductLibDto.Key.KEEP_PRIID1, keepPriId1));
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.LibCmd.GET_REL_LIB_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = relLibList.fromBuffer(recvBody, keyRef, ProductLibDto.getPdRelLibDto());
            if (m_rt != Errno.OK || keyRef.value != ProductLibDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    /**==============================================   商品库服务接口结束   ==============================================*/
}
