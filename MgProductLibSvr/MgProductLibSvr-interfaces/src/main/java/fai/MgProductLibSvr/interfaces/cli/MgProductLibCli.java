package fai.MgProductLibSvr.interfaces.cli;

import fai.MgProductLibSvr.interfaces.cmd.MgProductLibCmd;
import fai.MgProductLibSvr.interfaces.dto.ProductLibRelDto;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;
import fai.middleground.infutil.MgConfPool;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:15
 */
public class MgProductLibCli extends FaiClient {

    public MgProductLibCli(int flow, String name) {
        super(flow, name);
    }

    /**
     * <p> 初始化，开启配置中心的配置
     * @return
     */
    public boolean init() {
        return init("MgProductLibCli", true);
    }

    /**
     * 配置开启使用库服务
     * @return
     */
    public static boolean useProductLib() {
        Param mgSwitch = MgConfPool.getEnvConf("mgSwitch");
        if(Str.isEmpty(mgSwitch)) {
            return false;
        }
        return mgSwitch.getBoolean("useProductLib", false);
    }

    /**
     * 新增商品库信息，包含库业务的信息
     * @param libIdRef 接收返回的LibId
     * @param rlLibIdRef 接受返回的rlLibId
     */
    public int addProductLib(int aid, int tid, int unionPriId, Param info, Ref<Integer> libIdRef, Ref<Integer> rlLibIdRef) {
        if(!useProductLib()) {
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
            sendBody.putInt(ProductLibRelDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(ProductLibRelDto.Key.TID, tid);
            info.toBuffer(sendBody, ProductLibRelDto.Key.INFO, ProductLibRelDto.getAllInfoDto());

            //send and receive
            Param result = sendAndReceive(aid, MgProductLibCmd.LibCmd.ADD, sendBody, true);
            Boolean success = result.getBoolean("success");
            if (!success) {
                return m_rt;
            }

            FaiBuffer recvBody = (FaiBuffer) result.getObject("recvBody");
            if(libIdRef != null || rlLibIdRef != null) {
                Ref<Integer> keyRef = new Ref<Integer>();
                m_rt = recvBody.getInt(keyRef, rlLibIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductLibRelDto.Key.RL_LIB_ID) {
                    Log.logErr(m_rt, "recv rlLibId codec err");
                    return m_rt;
                }

                m_rt = recvBody.getInt(keyRef, libIdRef);
                if (m_rt != Errno.OK || keyRef.value != ProductLibRelDto.Key.LIB_ID) {
                    Log.logErr(m_rt, "recv LibId codec err");
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
     * 批量删除商品库，包含库业务的信息
     * @param rlLibIds 批量删除的库业务id。通过库业务id可以在server端查询到所有的LibId（库id）完成库的删除
     */
    public int delLibList(int aid, int unionPriId, FaiList<Integer> rlLibIds) {
        return 0;
    }

    /**
     * 批量修改商品库，包含库业务的信息
     * @param updaterList 保存了所有要更新的商品信息
     */
    public int setLibList(int aid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        return 0;
    }

    /**
     * 按条件查询库信息，查询到的库信息包含库业务的信息
     * @param searchArg 封装查询的条件
     * @param libList 保存查询到的所有库信息
     */
    public int getLibList(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> libList) {
        return 0;
    }

    /**
     * 获取所有的库业务表的信息
     * @param relLibList 保存查询到的库业务表的信息
     */
    public int getAllLibRel(int aid, int unionPriId, FaiList<Param> relLibList) {
        return 0;
    }

    /**
     * 根据查询条件从DB中查询库业务表的信息
     * @param searchArg 保存查询的条件
     * @param relLibList 保存根据查询条件在DB中查询到的结果
     */
    public int getLibRelFromDb(int aid, int unionPriId, SearchArg searchArg, FaiList<Param> relLibList) {
        return 0;
    }

    /**
     * 获取库业务表的数据状态
     */
    public int getLibRelDataStatus(int aid, int unionPriId, Param statusInfo) {
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