package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductGroupDto;
import fai.MgProductInfSvr.interfaces.dto.ProductLibDto;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
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
     * @param mgProductArg 非空
     *     MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                      .setAddInfo(info) // 必填
     *                      .build();
     * @param libIdRef 接收添加成功后的库id
     * @param rlLibIdRef 接收添加成功后的库业务id
     * @return {@link Errno}
     */
    public int addProductLib(MgProductArg mgProductArg, Ref<Integer> libIdRef, Ref<Integer> rlLibIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            Param info = mgProductArg.getAddInfo();

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
     * @param mgProductArg 非空
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                            .setDelRelLibIds(delRelLibIds) // 必填
     *                            .build();
     * @return {@link Errno}
     */
    public int delPdLibList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            FaiList<Integer> delRelLibIds = mgProductArg.getDelRelLibIds();

            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            if (Util.isEmptyList(delRelLibIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlLibIds is null;aid=%d;", aid);
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductLibDto.Key.TID, tid),
                    new Pair(ProductLibDto.Key.SITE_ID, siteId),
                    new Pair(ProductLibDto.Key.LGID, lgId),
                    new Pair(ProductLibDto.Key.KEEP_PRIID1, keepPriId1));
            delRelLibIds.toBuffer(sendBody, ProductLibDto.Key.RL_LIB_IDS);

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
     * @param mgProductArg 非空
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                       .setUpdaterList(updaterList)  // 必填 {@link fai.MgProductInfSvr.interfaces.entity.ProductLibEntity.Info}
     *                       .build();
     * @return {@link Errno}
     */
    public int setPdLibList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            FaiList<ParamUpdater> updaterList = mgProductArg.getUpdaterList();
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
     * @param mgProductArg
     *       MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                       .setSearchArg(searchArg)  // 选填
     *                       .build();
     * @param list 接收查询的结果
     * @return  {@link Errno}
     */
    public int getPdLibList(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            SearchArg searchArg = mgProductArg.getSearchArg();
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
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                    .setAddList(addList)           // 选填
     *                    .setUpdaterList(updaterList)   // 选填
     *                    .setDelRelLibIds(delRelLibIds) // 选填
     *                    .build();
     *       addList: 详见{@link fai.MgProductInfSvr.interfaces.entity.ProductLibEntity.Info}
     *       updaterList: 详见{@link fai.MgProductInfSvr.interfaces.entity.ProductLibEntity.Info}
     *       delRelLibIds: 库业务id集合
     * @param rlLibIdsRef 接收新增库的库业务id
     * @return {@link Errno}
     */
    public int unionSetLibList(MgProductArg mgProductArg, Ref<FaiList<Integer>> rlLibIdsRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            FaiList<Param> addInfoList = mgProductArg.getAddList();
            FaiList<ParamUpdater> updaterList = mgProductArg.getUpdaterList();
            FaiList<Integer> delRelLibIds = mgProductArg.getDelRelLibIds();
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
            if (delRelLibIds == null) {
                delRelLibIds = new FaiList<Integer>();
            }
            delRelLibIds.toBuffer(sendBody, ProductLibDto.Key.RL_LIB_IDS);

            boolean needRlLibIds = (rlLibIdsRef != null);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.LibCmd.UNION_SET_LIB_LIST, sendBody,
                    false, needRlLibIds);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            if (needRlLibIds) {
                Ref<Integer> keyRef = new Ref<Integer>();
                FaiList<Integer> rlLibIds = new FaiList<Integer>();
                rlLibIds.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductLibDto.Key.RL_LIB_IDS) {
                    Log.logErr(m_rt, "recv rlLibIds codec err");
                    return m_rt;
                }
                rlLibIdsRef.value = rlLibIds;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);

        }
    }


    /**
     * 获取所有的库业务表的数据
     * @param mgProductArg 非空
     *       MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                            .build();
     * @param relLibList 保存所有库业务表的数据
     * @return  {@link Errno}
     */
    public int getRlLibList(MgProductArg mgProductArg, FaiList<Param> relLibList) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            SearchArg searchArg = mgProductArg.getSearchArg();
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
            searchArg.toBuffer(sendBody, ProductLibDto.Key.SEARCH_ARG);
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
