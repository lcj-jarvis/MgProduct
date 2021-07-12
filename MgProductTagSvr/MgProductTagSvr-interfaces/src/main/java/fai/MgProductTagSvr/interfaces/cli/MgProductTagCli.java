package fai.MgProductTagSvr.interfaces.cli;

import fai.MgProductTagSvr.interfaces.cmd.MgProductTagCmd;
import fai.MgProductTagSvr.interfaces.dto.ProductTagRelDto;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.infutil.MgConfPool;

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
        return 0;
    }

    /**
     * 批量修改商品标签，包含标签业务的信息
     * @param updaterList 保存了所有要更新的商品标签信息
     */
    public int setTagList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        return 0;
    }

    /**
     * 按条件查询标签信息，查询到的标签信息包含标签业务的信息
     * @param searchArg 封装查询的条件
     * @param tagList 保存查询到的所有标签信息
     */
    public int getTagList(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> tagList) {
        return 0;
    }

    /**
     * 获取所有的标签业务表的信息,先查缓存再查DB
     * @param relTagList 保存查询到的库业务表的信息
     */
    public int getAllTagRel(int aid, int unionPriId, FaiList<Param> relTagList) {
        return 0;
    }

    /**
     * 根据查询条件从DB中查询标签业务表的信息
     * @param searchArg 保存查询的条件
     * @param relTagList 保存根据查询条件在DB中查询到的结果
     */
    public int getTagRelFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> relTagList) {
        return 0;
    }

    /**
     * 获取标签业务表的数据状态
     */
    public int getTagRelDataStatus(int aid, int unionPriId, Param statusInfo) {
        return 0;  
    }

    /**
     * 联合增删改
     * @param addInfoList  要添加的库信息
     * @param updaterList  要更新的标签信息
     * @param delRlTagIds  要删除的标签
     * @param rlTagIdsRef  接收新增标签的标签id
     */
    public int unionSetTagList(int aid, int tid, int unionPriId, FaiList<Param> addInfoList,
                               FaiList<ParamUpdater> updaterList,
                               FaiList<Integer> delRlTagIds,
                               FaiList<Integer> rlTagIdsRef) {
        return 0;
    }

    /**
     * 克隆数据
     */
    public int cloneData(int aid, int fromAid, FaiList<Param> cloneUnionPriIds) {
        return 0;
    }

    /**
     * 增量克隆
     */
    public int incrementalClone(int aid, int unionPriId, int fromAid, int fromUnionPriId) {
        return 0;
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