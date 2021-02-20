package fai.MgProductSearchSvr.interfaces.cli;

import fai.MgProductSearchSvr.interfaces.cmd.MgProductSearchCmd;
import fai.MgProductSearchSvr.interfaces.dto.MgProductSearchDto;
import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
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


    public int searchList(int aid, int tid, int unionPriId, int productCount, MgProductSearch mgProductSearch, FaiList<Param> list, Ref<Integer> totalSize){
        m_rt = Errno.ERROR;
        Oss.CliStat stat = new Oss.CliStat(m_name, m_flow);
        try {
            list.clear();
            // send
            FaiBuffer sendBody = new FaiBuffer(true);
            sendBody.putInt(MgProductSearchDto.Key.UNION_PRI_ID, unionPriId);
            sendBody.putInt(MgProductSearchDto.Key.TID, tid);
            sendBody.putInt(MgProductSearchDto.Key.TOTAL_SIZE, productCount);
            sendBody.putString(MgProductSearchDto.Key.SEARCH_PARAM_STRING, mgProductSearch.getSearchParam().toJson());
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
            list.fromBuffer(recvBody, keyRef, MgProductSearchDto.getDetailDto());
            if (m_rt != Errno.OK || keyRef.value != MgProductSearchDto.Key.SEARCH_LIST) {
                Log.logErr(m_rt, "recv codec err");
                return m_rt;
            }
            // recv total size
            if (totalSize != null) {
                recvBody.getInt(keyRef, totalSize);
                if (keyRef.value != MgProductSearchDto.Key.TOTAL_SIZE) {
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
}
