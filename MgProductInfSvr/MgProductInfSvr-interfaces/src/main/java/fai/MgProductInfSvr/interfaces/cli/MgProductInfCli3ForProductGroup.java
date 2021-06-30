package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductGroupDto;
import fai.MgProductInfSvr.interfaces.entity.ProductGroupEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;

public class MgProductInfCli3ForProductGroup extends MgProductInfCli2ForProductProp{
    public MgProductInfCli3ForProductGroup(int flow) {
        super(flow);
    }

    @Deprecated
    public int getPdGroupList(int aid, int tid, int siteId, int lgId, int keepPriId1, SearchArg searchArg, FaiList<Param> list) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setSearchArg(searchArg)
                .build();
        return getPdGroupList(mgProductArg, list);
    }

    @Deprecated
    public int getPdBindGroups(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, FaiList<Param> list) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdIds(rlPdIds)
                .build();
        return getPdBindGroups(mgProductArg, list);
    }

    @Deprecated
    public int addProductGroup(int aid, int tid, int siteId, int lgId, int keepPriId1, Param info, Ref<Integer> rlGroupIdRef) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setAddInfo(info)
                .build();
        return addProductGroup(mgProductArg, rlGroupIdRef);
    }

    @Deprecated
    public int setPdGroupList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<ParamUpdater> updaterList) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setUpdaterList(updaterList)
                .build();
        return setPdGroupList(mgProductArg);
    }

    @Deprecated
    public int delPdGroupList(int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlGroupIds) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlGroupIds(rlGroupIds)
                .build();
        return delPdGroupList(mgProductArg);
    }

    @Deprecated
    public int setPdBindGroup(int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Integer> addRlGroupIds, FaiList<Integer> delRlGroupIds) {
        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
                .setRlPdId(rlPdId)
                .setAddRlGroupIds(addRlGroupIds)
                .setDelRlGroupIds(delRlGroupIds)
                .build();
        return setPdBindGroup(mgProductArg);
    }

    /**==============================================   商品分类接口开始   ==============================================*/
    /**
     * 获取商品分类数据
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setSearchArg(searchArg)  // 选填
     *                 .build();
     * @param list 接收返回的数据列表
     * @return {@link Errno}
     */
    public int getPdGroupList(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            list.clear();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductGroupDto.Key.TID, tid), new Pair(ProductGroupDto.Key.SITE_ID, siteId), new Pair(ProductGroupDto.Key.LGID, lgId), new Pair(ProductGroupDto.Key.KEEP_PRIID1, keepPriId1));
            SearchArg searchArg = mgProductArg.getSearchArg();
            if (searchArg == null) {
                searchArg = new SearchArg();
            }
            searchArg.toBuffer(sendBody, ProductGroupDto.Key.SEARCH_ARG);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.GroupCmd.GET_GROUP_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductGroupDto.getPdGroupDto());
            if (m_rt != Errno.OK || keyRef.value != ProductGroupDto.Key.INFO_LIST) {
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
     * 根据rlPdIds获取商品绑定的分类数据
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds)  // 必填
     *                 .build();
     * @param list 返回数据列表
     * @return {@link Errno}
     */
    public int getPdBindGroups(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid), new Pair(ProductBasicDto.Key.SITE_ID, siteId), new Pair(ProductBasicDto.Key.LGID, lgId), new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if (Util.isEmptyList(rlPdIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg error");
                return m_rt;
            }
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
            int aid = mgProductArg.getAid();
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.GET_PD_BIND_GROUPS, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBasicDto.getBindGroupDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.BIND_GROUP_LIST) {
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
     * 新增商品分类
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setAddInfo(info) // 必填
     *                 .build();
     * @param rlGroupIdRef 接收返回的分类业务id
     * @return {@link Errno}
     */
    public int addProductGroup(MgProductArg mgProductArg, Ref<Integer> rlGroupIdRef) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            Param info = mgProductArg.getAddInfo();
            if (Str.isEmpty(info)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "info is null;aid=%d;", aid);
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductGroupDto.Key.TID, tid), new Pair(ProductGroupDto.Key.SITE_ID, siteId), new Pair(ProductGroupDto.Key.LGID, lgId), new Pair(ProductGroupDto.Key.KEEP_PRIID1, keepPriId1));
            info.toBuffer(sendBody, ProductGroupDto.Key.INFO, ProductGroupDto.getPdGroupDto());
            // send and recv
            boolean rlGroupIdRefNotNull = (rlGroupIdRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.GroupCmd.ADD_GROUP, sendBody, false, rlGroupIdRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (rlGroupIdRefNotNull) {
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlGroupIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductGroupDto.Key.RL_GROUP_ID) {
                    Log.logErr(m_rt, "recv rlGroupId codec err");
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
     * 修改商品分类
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setUpdaterList(updaterList)  // 必填 {@link ProductGroupEntity.GroupInfo}
     *                 .build();
     * @return {@link Errno}
     */
    public int setPdGroupList(MgProductArg mgProductArg) {
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
                Log.logErr(m_rt, "updaterList is null;aid=%d;", aid);
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductGroupDto.Key.TID, tid), new Pair(ProductGroupDto.Key.SITE_ID, siteId), new Pair(ProductGroupDto.Key.LGID, lgId), new Pair(ProductGroupDto.Key.KEEP_PRIID1, keepPriId1));
            updaterList.toBuffer(sendBody, ProductGroupDto.Key.UPDATERLIST, ProductGroupDto.getPdGroupDto());
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.GroupCmd.SET_GROUP_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 删除商品分类
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlGroupIds(rlGroupIds)  // 必填 分类业务id集合
     *                 .build();
     * @return {@link Errno}
     */
    public int delPdGroupList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> rlGroupIds = mgProductArg.getRlGroupIds();
            if (rlGroupIds == null || rlGroupIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlGroupIds is null;aid=%d;", aid);
                return m_rt;
            }
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductGroupDto.Key.TID, tid), new Pair(ProductGroupDto.Key.SITE_ID, siteId), new Pair(ProductGroupDto.Key.LGID, lgId), new Pair(ProductGroupDto.Key.KEEP_PRIID1, keepPriId1));
            rlGroupIds.toBuffer(sendBody, ProductGroupDto.Key.RL_GROUP_IDS);

            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.GroupCmd.DEL_GROUP_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 合并 增删改接口 (支持批量新增)
     *
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *              .setAddList(addList)         // 选填
     *              .setUpdaterList(updaterList) // 选填
     *              .setRlGroupIds(rlGroupIds)   // 选填
     *              .build();
     * addList: 详见{@link ProductGroupEntity.GroupInfo}
     * updaterList: 详见{@link ProductGroupEntity.GroupInfo}
     * rlGroupIds: 分类业务id集合
     * @param rlGroupIdsRef 接收返回的分类业务id集合
     * @return {@link Errno}
     */
    public int unionSetGroupList(MgProductArg mgProductArg, Ref<FaiList<Integer>> rlGroupIdsRef) {
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
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductGroupDto.Key.TID, tid), new Pair(ProductGroupDto.Key.SITE_ID, siteId), new Pair(ProductGroupDto.Key.LGID, lgId), new Pair(ProductGroupDto.Key.KEEP_PRIID1, keepPriId1));
            FaiList<Param> addList = mgProductArg.getAddList();
            if (addList == null) {
                addList = new FaiList<Param>();
            }
            addList.toBuffer(sendBody, ProductGroupDto.Key.INFO, ProductGroupDto.getPdGroupDto());
            FaiList<ParamUpdater> updaterList = mgProductArg.getUpdaterList();
            if (updaterList == null) {
                updaterList = new FaiList<ParamUpdater>();
            }
            updaterList.toBuffer(sendBody, ProductGroupDto.Key.UPDATERLIST, ProductGroupDto.getPdGroupDto());
            FaiList<Integer> delList = mgProductArg.getRlGroupIds();
            if (delList == null) {
                delList = new FaiList<Integer>();
            }
            delList.toBuffer(sendBody, ProductGroupDto.Key.RL_GROUP_IDS);
            // send and recv
            boolean rlGroupIdRefNotNull = (rlGroupIdsRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.GroupCmd.UNION_SET_GROUP_LIST, sendBody, false, rlGroupIdRefNotNull);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            if (rlGroupIdRefNotNull) {
                Ref<Integer> keyRef = new Ref<Integer>();
                FaiList<Integer> ids = new FaiList<Integer>();
                m_rt = ids.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductGroupDto.Key.RL_GROUP_ID) {
                    Log.logErr(m_rt, "recv rlGroupId codec err");
                    return m_rt;
                }
                rlGroupIdsRef.value = ids;
            }
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 设置商品绑定的分类数据(新增和删除)
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdId(rlPdId)                // 必填
     *                 .setAddRlGroupIds(addRlGroupIds)  // 选填
     *                 .setDelRlGroupIds(delRlGroupIds)  // 选填
     *                 .build();
     * @return {@link Errno}
     */
    public int setPdBindGroup(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> addRlGroupIds = mgProductArg.getAddRlGroupIds();
            if (addRlGroupIds == null) {
                addRlGroupIds = new FaiList<Integer>();
            }
            FaiList<Integer> delRlGroupIds = mgProductArg.getDelRlGroupIds();
            if (delRlGroupIds == null) {
                delRlGroupIds = new FaiList<Integer>();
            }
            if (addRlGroupIds.isEmpty() && delRlGroupIds.isEmpty()) {
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
            addRlGroupIds.toBuffer(sendBody, ProductBasicDto.Key.BIND_GROUP_IDS);
            delRlGroupIds.toBuffer(sendBody, ProductBasicDto.Key.DEL_BIND_GROUP_IDS);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.SET_PD_BIND_GROUP, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }
    /**----------------------------------------------   商品分类接口结束   ----------------------------------------------*/
}
