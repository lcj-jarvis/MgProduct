package fai.MgProductTagSvr.interfaces.cli;

import fai.MgProductTagSvr.interfaces.cmd.MgProductTagCmd;
import fai.MgProductTagSvr.interfaces.dto.ProductTagRelDto;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.infutil.MgConfPool;
import fai.comm.middleground.app.CloneDef;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:46
 */
public class MgProductTagCli extends FaiClient {

    public MgProductTagCli(int flow, String name) {
        super(flow, name);
    }

    /**
     * <p> 初始化，开启配置中心的配置
     * @return
     */
    public boolean init() {
        return init("MgProductTagCli", true);
    }

    /**
     * 配置开启使用标签服务
     * @return
     */
    public static boolean useProductTag() {
        Param mgSwitch = MgConfPool.getEnvConf("mgSwitch");
        if(Str.isEmpty(mgSwitch)) {
            return false;
        }
        return mgSwitch.getBoolean("useProductTag", false);
    }

    /**
     * 新增商品标签信息，包含标签业务的信息
     * @param tagIdRef 接收返回的TagId
     * @param rlTagIdRef 接受返回的rlTagId
     */
    public int addProductTag(int aid, int tid, int unionPriId, Param info, Ref<Integer> tagIdRef, Ref<Integer> rlTagIdRef) {
        if(!useProductTag()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if (info == null || info.isEmpty()) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "info is null");
                return m_rt;
            }

            //send data
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductTagRelDto.Key.TID, tid);
            info.toBuffer(sendBody, ProductTagRelDto.Key.INFO, ProductTagRelDto.getAllInfoDto());

            //send and receive
            Param result = sendAndReceive(aid, MgProductTagCmd.TagCmd.ADD, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            if(tagIdRef != null || rlTagIdRef != null) {
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlTagIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductTagRelDto.Key.RL_TAG_ID) {
                    Log.logErr(m_rt, "recv rlTagId codec err");
                    return m_rt;
                }

                m_rt = recvBody.getInt(keyRef, tagIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductTagRelDto.Key.TAG_ID) {
                    Log.logErr(m_rt, "recv TagId codec err");
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
     * 批量删除商品标签，包含标签业务的信息
     * @param rlTagIds 批量删除的标签业务id。通过标签业务id可以在server端查询到所有的TagId（标签id）完成标签的删除
     */
    public int delTagList(int aid, int unionPriId, FaiList<Integer> rlTagIds) {
        if(!useProductTag()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }
            if ( Util.isEmptyList(rlTagIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.UNION_PRI_ID, unionPriId);
            rlTagIds.toBuffer(sendBody, ProductTagRelDto.Key.RL_TAG_IDS);

            //send and receive
            sendAndReceive(aid, MgProductTagCmd.TagCmd.BATCH_DEL, sendBody, false);

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 批量修改商品标签，包含标签业务的信息
     * @param updaterList 保存了所有要更新的商品标签信息
     */
    public int setTagList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        if(!useProductTag()) {
            return Errno.OK;
        }
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
                Log.logErr(m_rt, "list is null");
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.UNION_PRI_ID, unionPriId);
            updaterList.toBuffer(sendBody, ProductTagRelDto.Key.UPDATERLIST, ProductTagRelDto.getAllInfoDto());

            sendAndReceive(aid, MgProductTagCmd.TagCmd.BATCH_SET, sendBody,false);

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 按条件查询标签信息，查询到的标签信息包含标签业务的信息
     * @param searchArg 封装查询的条件
     * @param tagList 保存查询到的所有标签信息
     */
    public int getTagList(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> tagList) {
        if(!useProductTag()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (tagList == null) {
                tagList = new FaiList<Param>();
            }
            tagList.clear();

            if (searchArg == null) {
                searchArg = new SearchArg();
            }

            // send contents
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.UNION_PRI_ID, unionPriId);
            searchArg.toBuffer(sendBody, ProductTagRelDto.Key.SEARCH_ARG);

            //send and receive
            Param result = sendAndReceive(aid, MgProductTagCmd.TagCmd.GET_LIST, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = tagList.fromBuffer(recvBody, keyRef, ProductTagRelDto.getAllInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductTagRelDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductTagRelDto.Key.TOTAL_SIZE) {
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

    /**
     * 获取所有的标签业务表的信息,先查缓存再查DB
     * @param relTagList 保存查询到的标签业务表的信息
     */
    public int getAllTagRel(int aid, int unionPriId, FaiList<Param> relTagList) {
        return getTagRelByCondition(aid, unionPriId, null, relTagList);
    }
    
    /**
     * 根据查询条件从DB中查询标签业务表的信息
     * @param searchArg 保存查询的条件
     * @param relTagList 保存根据查询条件在DB中查询到的结果
     */
    public int getTagRelFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> relTagList) {
        if (searchArg == null) {
            searchArg = new SearchArg();
        }
        return getTagRelByCondition(aid, unionPriId, searchArg, relTagList);
    }

    /**
     * 根据条件查询标签业务表的信息
     */
    private  int getTagRelByCondition(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> relTagList) {
        if(!useProductTag()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            relTagList.clear();
            int command = MgProductTagCmd.TagCmd.GET_ALL_REL;
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.UNION_PRI_ID, unionPriId);

            if (searchArg != null) {
                //调用svr的getTagRelFromDb方法
                command = MgProductTagCmd.TagCmd.SEARCH_REL;
                searchArg.toBuffer(sendBody, ProductTagRelDto.Key.SEARCH_ARG);
            }else {
                //调用svr的getAllTagRel方法
                searchArg = new SearchArg();
            }

            Param result = sendAndReceive(aid, command, sendBody,true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = relTagList.fromBuffer(recvBody, keyRef, ProductTagRelDto.getInfoDto());
            if (m_rt != Errno.OK || keyRef.value != ProductTagRelDto.Key.INFO_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }

            // recv total size
            if (searchArg.totalSize != null) {
                recvBody.getInt(keyRef, searchArg.totalSize);
                if (keyRef.value != ProductTagRelDto.Key.TOTAL_SIZE) {
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


    /**
     * 获取标签业务表的数据状态
     */
    public int getTagRelDataStatus(int aid, int unionPriId, Param statusInfo) {
        if(!useProductTag()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            statusInfo.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.UNION_PRI_ID, unionPriId);

            //send and receive
            Param result = sendAndReceive(aid,MgProductTagCmd.TagCmd.GET_REL_DATA_STATUS,sendBody,true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            m_rt = statusInfo.fromBuffer(recvBody, keyRef, DataStatus.Dto.getDataStatusDto());
            if (m_rt != Errno.OK || keyRef.value != ProductTagRelDto.Key.DATA_STATUS) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK), m_rt);

        }
    }

    /**
     * 联合增删改
     * @param addInfoList  要添加的标签信息
     * @param updaterList  要更新的标签信息
     * @param delRlTagIds  要删除的标签
     * @param rlTagIdsRef  接收新增标签的标签id
     */
    public int unionSetTagList(int aid, int tid, int unionPriId, FaiList<Param> addInfoList,
                               FaiList<ParamUpdater> updaterList,
                               FaiList<Integer> delRlTagIds,
                               FaiList<Integer> rlTagIdsRef) {
        if(!useProductTag()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error");
                return m_rt;
            }

            //初始化rlTagIdsRef
            if (rlTagIdsRef == null) {
                rlTagIdsRef = new FaiList<Integer>();
            }
            rlTagIdsRef.clear();

            // 组装sendBody
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductTagRelDto.Key.TID, tid);
            if (Util.isEmptyList(addInfoList)) {
                addInfoList = new FaiList<Param>();
            }
            addInfoList.toBuffer(sendBody, ProductTagRelDto.Key.INFO_LIST, ProductTagRelDto.getAllInfoDto());
            if (updaterList == null) {
                updaterList = new FaiList<ParamUpdater>();
            }
            updaterList.toBuffer(sendBody, ProductTagRelDto.Key.UPDATERLIST, ProductTagRelDto.getAllInfoDto());
            if (delRlTagIds == null) {
                delRlTagIds = new FaiList<Integer>();
            }
            delRlTagIds.toBuffer(sendBody, ProductTagRelDto.Key.RL_TAG_IDS);

            //发送数据
            Param result = sendAndReceive(aid, MgProductTagCmd.TagCmd.UNION_SET_TAG_LIST, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            //接收插入成功后的reTagId
            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            if (!Util.isEmptyList(addInfoList)) {
                Ref<Integer> keyRef = new Ref<Integer>();
                rlTagIdsRef.fromBuffer(recvBody, keyRef);
                if (m_rt != Errno.OK || keyRef.value != ProductTagRelDto.Key.RL_TAG_IDS) {
                    Log.logErr(m_rt, "recv rlTagIds codec err");
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
     * 克隆数据
     * @param fromAid 从哪个aid下克隆
     * @param cloneUnionPriIds 保存fromUnionPriId（从哪个unionId下克隆）和toUnionPriId（克隆到哪个unionId下）
     */
    public int cloneData(int aid, int fromAid, FaiList<Param> cloneUnionPriIds) {
        if (!useProductTag()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0 || Util.isEmptyList(cloneUnionPriIds)) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;fromAid=%d;cloneUids=%s;", aid, fromAid, cloneUnionPriIds);
                return m_rt;
            }

            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.FROM_AID, fromAid);
            //cloneUnionPriIds.toBuffer(sendBody, ProductTagRelDto.Key.CLONE_UNION_PRI_IDS, CloneDef.Dto.getDto());
            //发送数据
            sendAndReceive(aid, MgProductTagCmd.TagCmd.CLONE, sendBody, false);

            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }

    /**
     * 增量克隆：从fromAid、fromUnionPriId查看对应要克隆的数据，如果aid、unionPriId已经存在的就不进行克隆，
     * 克隆未存在的。移除克隆数据的tagId，设置新的tagId，新的tagId在已经存在的tagId下开始自增。
     * @param aid 克隆到哪个aid下
     * @param unionPriId 克隆到哪个uid下
     * @param fromAid 从哪个aid下开始克隆
     * @param fromUnionPriId 从哪个unionPriId下开始克隆
     * @return
     */
    public int incrementalClone(int aid, int unionPriId, int fromAid, int fromUnionPriId) {
        if (!useProductTag()) {
            return Errno.OK;
        }
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if (aid == 0) {
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "args error;aid=%d;uid=%d;fromAid=%d;fromUid=%d;", aid, unionPriId, fromAid, fromUnionPriId);
                return m_rt;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(ProductTagRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductTagRelDto.Key.FROM_AID, fromAid);
            sendBody.putInt(ProductTagRelDto.Key.FROM_UNION_PRI_ID, fromUnionPriId);
            //发送数据
            sendAndReceive(aid, MgProductTagCmd.TagCmd.INCR_CLONE, sendBody, false);

            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end(m_rt != Errno.OK, m_rt);
        }
    }


    /**
     * 发送和接收数据，并且验证发送和接收是否成功
     * @param sendBody 发送的数据体
     * @param verifyReceiveBody true表示要验证接收的数据是否为null，false表示不用验证
     * @return true 表示发送和接收成功，false表示发送和接收失败
     */
    private Param sendAndReceive(int aid, int command, FaiBuffer sendBody, boolean verifyReceiveBody) {

        Param param = new Param(true);
        FaiProtocol sendProtocol = new FaiProtocol();
        sendProtocol.setAid(aid);
        sendProtocol.setCmd(command);
        sendProtocol.addEncodeBody(sendBody);
        //send
        m_rt = send(sendProtocol);
        if (m_rt != Errno.OK) {
            Log.logErr(m_rt, "send err");
            return param.setBoolean("success", false);
        }

        // recv
        FaiProtocol recvProtocol = new FaiProtocol();
        m_rt = recv(recvProtocol);
        if (m_rt != Errno.OK) {
            Log.logErr(m_rt, "recv err");
            return param.setBoolean("success", false);
        }
        m_rt = recvProtocol.getResult();
        if (m_rt != Errno.OK) {
            if (m_rt != Errno.NOT_FOUND) {
                Log.logErr(m_rt, "recv result err");
            }
            return param.setBoolean("success", false);
        }

        if (!verifyReceiveBody) {
            return param.setBoolean("success", true);
        }

        FaiBuffer recvBody = recvProtocol.getDecodeBody();
        if (recvBody == null) {
            m_rt = Errno.CODEC_ERROR;
            Log.logErr(m_rt, "recv body null");
            return param.setBoolean("success", false);
        }

        return param.setBoolean("success", true).setObject("recvBody", recvBody);
    }
}