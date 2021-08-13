package fai.MgProductSearchSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSearchSvr.interfaces.cmd.MgProductSearchCmd;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;

public class MgProductSearchCli extends FaiClient {

    public MgProductSearchCli(int flow) {
        super(flow, "MgProductSearchCli");
    }

    /**
     * <p> 初始化
     * @return
     */
    public boolean init() {
        return init("MgProductSearchCli", true);
    }


    public int searchList(int aid, int tid, int unionPriId, int productCount, String esSearchParamString, String searchParamString, Param searchResult){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            if(searchResult == null){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "searchResult == null error");
                return Errno.ARGS_ERROR;
            }
            searchResult.clear();
            // 如果没有筛选条件，返回空数据，防止误调用  包括 {} 这个判断
            if(Str.isEmpty(searchParamString) || searchParamString.length() == 2){
                m_rt = Errno.ARGS_ERROR;
                Log.logErr(m_rt, "mgProductSearch == null error");
                return Errno.ARGS_ERROR;
            }
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(MgProductSearchDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(MgProductSearchDto.Key.TID, tid);
            sendBody.putInt(MgProductSearchDto.Key.PRODUCT_COUNT, productCount);
            sendBody.putString(MgProductSearchDto.Key.ES_SEARCH_PARAM_STRING, esSearchParamString);
            sendBody.putString(MgProductSearchDto.Key.SEARCH_PARAM_STRING, searchParamString);
            FaiProtocol sendProtocol = new FaiProtocol();
            sendProtocol.setAid(aid);
            sendProtocol.setCmd(MgProductSearchCmd.SearchCmd.SEARCH_LIST);
            sendProtocol.addEncodeBody(sendBody);

            m_rt = send(sendProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "send err");
                return m_rt;
            }

            // recv
            FaiProtocol recvProtocol = new FaiProtocol();
            m_rt = recv(recvProtocol);
            if (m_rt != Errno.OK) {
                Log.logErr(m_rt, "recv err");
                return m_rt;
            }
            m_rt = recvProtocol.getResult();
            if (m_rt != Errno.OK) {
                if (m_rt != Errno.NOT_FOUND) {
                    Log.logErr(m_rt, "recv result err");
                }
                return m_rt;
            }

            FaiBuffer recvBody = recvProtocol.getDecodeBody();
            if (recvBody == null) {
                m_rt = Errno.CODEC_ERROR;
                Log.logErr(m_rt, "recv body null");
                return m_rt;
            }
            // recv info
            Ref<Integer> keyRef = new Ref<Integer>();
            searchResult.fromBuffer(recvBody, keyRef, MgProductSearchDto.getProductSearchDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductSearchDto.Key.RESULT_INFO) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            m_rt = Errno.OK;
            return m_rt;
        } finally {
            close();
            stat.end((m_rt != Errno.OK) && (m_rt != Errno.NOT_FOUND), m_rt);
        }
    }
}
