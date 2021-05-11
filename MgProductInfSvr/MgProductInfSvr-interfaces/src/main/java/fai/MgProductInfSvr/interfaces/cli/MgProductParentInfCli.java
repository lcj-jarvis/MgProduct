package fai.MgProductInfSvr.interfaces.cli;

import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.Errno;
import fai.comm.util.FaiBuffer;
import fai.comm.util.Log;
import fai.comm.util.Pair;

public class MgProductParentInfCli extends FaiClient {
    public MgProductParentInfCli(int flow) {
        super(flow, "MgProductInfCli");
    }
    public boolean init() {
        return init("MgProductInfCli", true);
    }

    /**
     *  封装 packaging send data 方法
     * @param pairs
     *
     */
    public FaiBuffer getDefaultFaiBuffer(Pair ...pairs){
        FaiBuffer sendBody = new FaiBuffer(true);
        if(pairs == null || pairs.length <= 0){
            return sendBody;
        }
        for(int i = 0; i < pairs.length; i++){
            if(pairs[i] != null){
                sendBody.putInt((Integer) pairs[i].first, (Integer)pairs[i].second);
            }
        }
        return sendBody;
    }


    public FaiBuffer sendAndRecv(int aid, int cmd, FaiBuffer sendBody, boolean checkNotFound){
        return sendAndRecv(aid, cmd, sendBody, checkNotFound, true);
    }
    /**
     *  封装send 和 recv 的方法，方法是否正确被调用，外部根据 m_rt 来判断就行
     * @param aid
     * @param cmd
     * @param sendBody
     * @param checkNotFound , 默认是 false, 一般 get 数据操作是 true，其他的操作是 false
     * @param decodeRecvBody, 默认是 true , 是否进行对接的包进行解包
     *
     */
    public FaiBuffer sendAndRecv(int aid, int cmd, FaiBuffer sendBody, boolean checkNotFound, boolean decodeRecvBody){
        FaiProtocol sendProtocol = new FaiProtocol();
        sendProtocol.setAid(aid);
        sendProtocol.setCmd(cmd);
        sendProtocol.addEncodeBody(sendBody);

        m_rt = send(sendProtocol);
        if (m_rt != Errno.OK) {
            Log.logErr(m_rt, "send err;aid=%d", aid);
            return null;
        }

        // recv
        FaiProtocol recvProtocol = new FaiProtocol();
        m_rt = recv(recvProtocol);
        if (m_rt != Errno.OK) {
            Log.logErr(m_rt, "recv err;aid=%d", aid);
            return null;
        }
        m_rt = recvProtocol.getResult();
        if (m_rt != Errno.OK) {
            if(checkNotFound){
                if (m_rt != Errno.NOT_FOUND) {
                    Log.logErr(m_rt, "recv result err;aid=%d", aid);
                }
            }
            return null;
        }
        FaiBuffer recvBody = null;
        if(decodeRecvBody){
            recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body=null;aid=%d", aid);
                return null;
            }
        }
        return recvBody;
    }
}
