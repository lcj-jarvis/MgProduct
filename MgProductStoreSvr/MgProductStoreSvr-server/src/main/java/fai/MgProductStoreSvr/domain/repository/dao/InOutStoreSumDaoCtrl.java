package fai.MgProductStoreSvr.domain.repository.dao;

import fai.MgProductStoreSvr.domain.repository.TableDBMapping;
import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class InOutStoreSumDaoCtrl extends DaoCtrl {
    private InOutStoreSumDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }


    public static InOutStoreSumDaoCtrl getInstance(int flow, int aid) {
        if(m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new InOutStoreSumDaoCtrl(flow, aid);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }
    @Override
    protected String getTableName(){
        return TABLE_ENUM.getTable() + "_"+ String.format("%04d", aid%1000);
    }


    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_IN_OUT_STORE_SUM;
}
