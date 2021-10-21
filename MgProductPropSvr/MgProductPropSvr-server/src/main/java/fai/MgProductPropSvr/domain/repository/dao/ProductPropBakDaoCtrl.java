package fai.MgProductPropSvr.domain.repository.dao;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductPropBakDaoCtrl extends DaoCtrl {

	public ProductPropBakDaoCtrl(int flow, int aid) {
		super(flow, aid);
	}

	@Override
	public String getTableName() {
		return TABLE_PREFIX + "_" + String.format("%04d", getAid() % 1000);
	}

	@Override
	protected DaoPool getDaoPool() {
		return m_daoPool;
	}

	public static ProductPropBakDaoCtrl getInstance(int flow, int aid) {
		if(m_daoPool == null) {
			Log.logErr("m_daoPool is not init;");
			return null;
		}
		return new ProductPropBakDaoCtrl(flow, aid);
	}

	public static void init(DaoPool daoPool) {
		m_daoPool = daoPool;
	}

	private static DaoPool m_daoPool;
	private static final String TABLE_PREFIX = "mgProductProp_bak";
}
