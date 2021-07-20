package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductTagDto;
import fai.MgProductInfSvr.interfaces.utils.MgProductArg;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;

/**
 * @author LuChaoJi
 * @date 2021-07-19 11:32
 */
public class MgProductInfCli7ForProductTag extends MgProductInfCli6ForProductLib{
    public MgProductInfCli7ForProductTag(int flow) {
        super(flow);
    }

    /**==============================================   商品标签服务接口开始   ==============================================*/
    /**
     * 新增商品标签
     * @param mgProductArg 非空
     *     MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                      .setAddInfo(info) // 必填
     *                      .build();
     * @param tagIdRef 接收添加成功后的标签id
     * @param rlTagIdRef 接收添加成功后的标签业务id
     * @return {@link Errno}
     */
    public int addProductTag(MgProductArg mgProductArg, Ref<Integer> tagIdRef, Ref<Integer> rlTagIdRef) {
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
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTagDto.Key.TID, tid),
                    new Pair(ProductTagDto.Key.SITE_ID, siteId),
                    new Pair(ProductTagDto.Key.LGID, lgId),
                    new Pair(ProductTagDto.Key.KEEP_PRIID1, keepPriId1));
            info.toBuffer(sendBody, ProductTagDto.Key.INFO, ProductTagDto.getPdTagDto());

            // send and recv
            boolean needReceive = (tagIdRef != null) || (rlTagIdRef != null);
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.TagCmd.ADD_TAG, sendBody,
                    false, needReceive);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            Ref<Integer> keyRef = new Ref<Integer>();
            if (tagIdRef != null) {
                m_rt = recvBody.getInt(keyRef, rlTagIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductTagDto.Key.TAG_ID) {
                    Log.logErr(m_rt, "recv tagId codec err");
                    return m_rt;
                }
            }
            if (rlTagIdRef != null) {
                m_rt = recvBody.getInt(keyRef, tagIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductTagDto.Key.RL_TAG_ID) {
                    Log.logErr(m_rt, "recv rlTagId codec err");
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
     * 删除商品标签
     * @param mgProductArg 非空
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                            .setDelRlTagIds(delRelTagIds) // 必填
     *                            .build();
     * @return {@link Errno}
     */
    public int delPdTagList(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            FaiList<Integer> delRlTagIds = mgProductArg.getDelRlTagIds();

            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            if (Util.isEmptyList(delRlTagIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlTagIds is null;aid=%d;", aid);
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTagDto.Key.TID, tid),
                    new Pair(ProductTagDto.Key.SITE_ID, siteId),
                    new Pair(ProductTagDto.Key.LGID, lgId),
                    new Pair(ProductTagDto.Key.KEEP_PRIID1, keepPriId1));
            delRlTagIds.toBuffer(sendBody, ProductTagDto.Key.RL_TAG_IDS);

            // send and recv
            sendAndRecv(aid, MgProductInfCmd.TagCmd.DEL_TAG_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);

        }
    }

    /**
     * 修改商品标签
     * @param mgProductArg 非空
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                       .setUpdaterList(updaterList)  // 必填 {@link fai.MgProductInfSvr.interfaces.entity.ProductTagEntity.Info}
     *                       .build();
     * @return {@link Errno}
     */
    public int setPdTagList(MgProductArg mgProductArg) {
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
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTagDto.Key.TID, tid),
                    new Pair(ProductTagDto.Key.SITE_ID, siteId),
                    new Pair(ProductTagDto.Key.LGID, lgId),
                    new Pair(ProductTagDto.Key.KEEP_PRIID1, keepPriId1));
            updaterList.toBuffer(sendBody, ProductTagDto.Key.UPDATERLIST, ProductTagDto.getPdTagDto());
            // send and recv
            sendAndRecv(aid, MgProductInfCmd.TagCmd.SET_TAG_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);

        }
    }

    /**
     * 查询商品标签的数据
     * @param mgProductArg
     *       MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                       .setSearchArg(searchArg)  // 选填
     *                       .build();
     * @param list 接收查询的结果
     * @return  {@link Errno}
     */
    public int getPdTagList(MgProductArg mgProductArg, FaiList<Param> list) {
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
            if (list == null) {
                list = new FaiList<Param>();
            }
            list.clear();

            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTagDto.Key.TID, tid),
                    new Pair(ProductTagDto.Key.SITE_ID, siteId),
                    new Pair(ProductTagDto.Key.LGID, lgId),
                    new Pair(ProductTagDto.Key.KEEP_PRIID1, keepPriId1));
            if (searchArg != null) {
                searchArg.toBuffer(sendBody, ProductTagDto.Key.SEARCH_ARG);
            }
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.TagCmd.GET_TAG_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductTagDto.getPdTagDto());
            if (m_rt != Errno.OK || keyRef.value != ProductTagDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if (searchArg != null && searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductTagDto.Key.TOTAL_SIZE) {
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
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
     * 联合增删改。先删除，再修改，最后添加
     * @param mgProductArg
     * MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                    .setAddList(addList)           // 选填
     *                    .setUpdaterList(updaterList)   // 选填
     *                    .setDelRlTagIds(delRlTagIds) // 选填
     *                    .build();
     *       addList: 详见{@link fai.MgProductInfSvr.interfaces.entity.ProductTagEntity.Info}
     *       updaterList: 详见{@link fai.MgProductInfSvr.interfaces.entity.ProductTagEntity.Info}
     *       delRlTagIds: 标签业务id集合
     * @param rlTagIdsRef 接收新增标签的标签业务id
     * @return {@link Errno}
     */
    public int unionSetTagList(MgProductArg mgProductArg, Ref<FaiList<Integer>> rlTagIdsRef) {
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
            FaiList<Integer> delRlTagIds = mgProductArg.getDelRlTagIds();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTagDto.Key.TID, tid),
                    new Pair(ProductTagDto.Key.SITE_ID, siteId),
                    new Pair(ProductTagDto.Key.LGID, lgId),
                    new Pair(ProductTagDto.Key.KEEP_PRIID1, keepPriId1));

            if (Util.isEmptyList(addInfoList)) {
                addInfoList = new FaiList<Param>();
            }
            addInfoList.toBuffer(sendBody, ProductTagDto.Key.INFO_LIST, ProductTagDto.getPdTagDto());
            if (updaterList == null) {
                updaterList = new FaiList<ParamUpdater>();
            }
            updaterList.toBuffer(sendBody, ProductTagDto.Key.UPDATERLIST, ProductTagDto.getPdTagDto());
            if (delRlTagIds == null) {
                delRlTagIds = new FaiList<Integer>();
            }
            delRlTagIds.toBuffer(sendBody, ProductTagDto.Key.RL_TAG_IDS);

            boolean needRlTagIds = (rlTagIdsRef != null);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.TagCmd.UNION_SET_TAG_LIST, sendBody,
                    false, needRlTagIds);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            if (needRlTagIds) {
                Ref<Integer> keyRef = new Ref<Integer>();
                FaiList<Integer> rlTagIds = new FaiList<Integer>();
                rlTagIds.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductTagDto.Key.RL_TAG_IDS) {
                    Log.logErr(m_rt, "recv rlTagIds codec err");
                    return m_rt;
                }
                rlTagIdsRef.value = rlTagIds;
            }

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);

        }
    }

    /**
     * 按照条件查询标签业务表的数据
     * @param mgProductArg 非空
     *       MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                     .setSearchArg(searchArg)  // 选填
     *                     .build();
     * @param rlTagList 保存所有标签业务表的数据
     * @return  {@link Errno}
     */
    public int getPdRlTagList(MgProductArg mgProductArg, FaiList<Param> rlTagList) {
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
            if (searchArg == null) {
                searchArg = new SearchArg();
            }
            if (rlTagList == null) {
                rlTagList = new FaiList<Param>();
            }
            rlTagList.clear();

            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTagDto.Key.TID, tid),
                    new Pair(ProductTagDto.Key.SITE_ID, siteId),
                    new Pair(ProductTagDto.Key.LGID, lgId),
                    new Pair(ProductTagDto.Key.KEEP_PRIID1, keepPriId1));
            searchArg.toBuffer(sendBody, ProductTagDto.Key.SEARCH_ARG);
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.TagCmd.GET_REL_TAG_LIST, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }

            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = rlTagList.fromBuffer(recvBody, keyRef, ProductTagDto.getPdRelTagDto());
            if (m_rt != Errno.OK || keyRef.value != ProductTagDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductTagDto.Key.TOTAL_SIZE) {
                    m_rt = Errno.CODEC_ERROR;
                    Log.logErr(m_rt, "recv total size null");
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
     * 根据rlPdIds获取商品绑定的标签数据
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds)  // 必填
     *                 .build();
     * @param list 返回数据列表
     * @return {@link Errno}
     */
    public int getPdBindTags(MgProductArg mgProductArg, FaiList<Param> list) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid),
                    new Pair(ProductBasicDto.Key.SITE_ID, siteId),
                    new Pair(ProductBasicDto.Key.LGID, lgId),
                    new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();
            if (Util.isEmptyList(rlPdIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "arg error");
                return m_rt;
            }
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);
            int aid = mgProductArg.getAid();
            // send and recv
            FaiBuffer recvBody = sendAndRecv(aid, MgProductInfCmd.BasicCmd.GET_PD_BIND_TAGS, sendBody, true);
            if (m_rt != Errno.OK) {
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = list.fromBuffer(recvBody, keyRef, ProductBasicDto.getBindTagDto());
            if (m_rt != Errno.OK || keyRef.value != ProductBasicDto.Key.BIND_TAG_LIST) {
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
     * 设置商品绑定的标签数据(删除和修改)
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdId(rlPdId)                // 必填
     *                 .setAddRlTagIds(addRlTagIds)  // 选填
     *                 .setDelRlTagIds(delRlTagIds)  // 选填
     *                 .build();
     * @return {@link Errno}
     */
    public int setPdBindTag(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            FaiList<Integer> addRlTagIds = mgProductArg.getAddRlTagIds();
            if (addRlTagIds == null) {
                addRlTagIds = new FaiList<Integer>();
            }
            FaiList<Integer> delRlTagIds = mgProductArg.getDelRlTagIds();
            if (delRlTagIds == null) {
                delRlTagIds = new FaiList<Integer>();
            }
            if (addRlTagIds.isEmpty() && delRlTagIds.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;addList and delList all empty");
                return m_rt;
            }

            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductBasicDto.Key.TID, tid),
                    new Pair(ProductBasicDto.Key.SITE_ID, siteId),
                    new Pair(ProductBasicDto.Key.LGID, lgId),
                    new Pair(ProductBasicDto.Key.KEEP_PRIID1, keepPriId1));
            int rlPdId = mgProductArg.getRlPdId();
            sendBody.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdId);
            addRlTagIds.toBuffer(sendBody, ProductBasicDto.Key.BIND_TAG_IDS);
            delRlTagIds.toBuffer(sendBody, ProductBasicDto.Key.DEL_BIND_TAG_IDS);
            // send and recv
            sendAndRecv(aid, MgProductInfCmd.BasicCmd.SET_PD_BIND_TAG, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 删除商品绑定的标签数据(根据rlPdIds)
     * @param mgProductArg
     *        MgProductArg mgProductArg = new MgProductArg.Builder(aid, tid, siteId, lgId, keepPriId1)
     *                 .setRlPdIds(rlPdIds)               // 必填
     *                 .build();
     * @return {@link Errno}
     */
    public int delPdBindTag(MgProductArg mgProductArg) {
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            int aid = mgProductArg.getAid();
            int tid = mgProductArg.getTid();
            int siteId = mgProductArg.getSiteId();
            int lgId = mgProductArg.getLgId();
            int keepPriId1 = mgProductArg.getKeepPriId1();
            FaiList<Integer> rlPdIds = mgProductArg.getRlPdIds();

            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            if (Util.isEmptyList(rlPdIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "rlPdIds is null;aid=%d;", aid);
                return m_rt;
            }
            // packaging send data
            FaiBuffer sendBody = getDefaultFaiBuffer(new Pair(ProductTagDto.Key.TID, tid),
                    new Pair(ProductTagDto.Key.SITE_ID, siteId),
                    new Pair(ProductTagDto.Key.LGID, lgId),
                    new Pair(ProductTagDto.Key.KEEP_PRIID1, keepPriId1));
            rlPdIds.toBuffer(sendBody, ProductBasicDto.Key.RL_PD_IDS);

            // send and recv
            sendAndRecv(aid, MgProductInfCmd.TagCmd.DEL_TAG_LIST, sendBody, false, false);
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);

        }
    }
    /**==============================================   商品标签服务接口结束   ==============================================*/

}
