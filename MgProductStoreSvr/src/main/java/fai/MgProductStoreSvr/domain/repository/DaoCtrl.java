package fai.MgProductStoreSvr.domain.repository;

import fai.comm.middleground.repository.DaoProxy;
import fai.comm.util.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public abstract class DaoCtrl {
	public DaoCtrl(int flow, int aid) {
		this.flow = flow;
		this.aid = aid;
	}
	public DaoCtrl(int flow, int aid, Dao dao) {
		this.flow = flow;
		this.aid = aid;
		this.m_daoOpened = true;
		this.m_dao = dao;
	}

	public static void init(DaoProxy daoProxy) {
		m_daoProxy = daoProxy;
		defaultGroup = m_daoProxy.getDefaulGroup();
	}
	protected int openDao(Dao dao) {
		if(m_daoOpened) {
			return Errno.OK;
		}
		if(dao == null){
			m_dao = getDaoPool().getDao();
			LinkedList<DaoCtrl> daoCtrls = openedDaoRecord.get(Thread.currentThread().getId());
			if(daoCtrls == null){
				daoCtrls = new LinkedList<>();
				openedDaoRecord.put(Thread.currentThread().getId(), daoCtrls);
			}
			daoCtrls.add(this);
		}else{
			m_dao = dao;
			useCommDao = true;
		}
		if(m_dao == null) {
			Log.logErr("get dao err;");
			return Errno.DAO_ERROR;
		}
		m_daoOpened = true;
		return Errno.OK;
	}
	protected int openDao() {
		return openDao(null);
	}

	public void closeDao() {
		if(m_dao != null) {
			if(!useCommDao){
				m_dao.close();
				LinkedList<DaoCtrl> daoCtrls = openedDaoRecord.get(Thread.currentThread().getId());
				if(daoCtrls != null){
					Iterator<DaoCtrl> iterator = daoCtrls.iterator();
					while (iterator.hasNext()){
						if(this.equals(iterator.next())){
							iterator.remove();
						}
					}
				}
			}
			m_dao = null;
			m_daoOpened = false;
		}
	}
	public int setAutoCommit(boolean autoCommit) {
		int rt = openDao();
		if(rt != Errno.OK){
			return rt;
		}
		return m_dao.setAutoCommit(autoCommit);
	}
	public void commit(){
		if(m_dao != null){
			m_dao.commit();
		}
	}
	public void rollback(){
		if(m_dao != null){
			m_dao.rollback();
		}
	}

	/**
	 * 用于Handler调用
	 */
	public static void closeDao4End(){
		LinkedList<DaoCtrl> daoCtrls = openedDaoRecord.remove(Thread.currentThread().getId());
		if(daoCtrls == null){
			return;
		}
		for (DaoCtrl daoCtrl : daoCtrls) {
			daoCtrl.closeDao();
		}
	}
	/**
	 * 关闭系统服务时调用
	 */
	public static void destroy(){
		if(m_daoProxy != null){
			m_daoProxy.destroy();
		}
	}

	public int insert(Param data) {
		return insert(data, null);
	}

	public int insert(Param data, Ref<Integer> autoIdRef) {
		int rt;
		if(Str.isEmpty(data)) {
			rt = Errno.ERROR;
			Log.logErr("insert arg is empty;data=%s", data);
			return rt;
		}
		rt = openDao();
		if(rt != Errno.OK) {
			return rt;
		}
		rt = m_dao.insertReturnAutoId(getTableName(), data, autoIdRef);
		return rt;
	}

	/**
	 * @param setDataListElementNull 是否將dataList中的元素设置为null, for gc
	 */
	public int batchInsert(FaiList<Param> dataList, FaiList<Integer> queueIdList, boolean setDataListElementNull) {
		int rt;

		if(dataList == null || dataList.isEmpty()) {
			rt = Errno.ERROR;
			Log.logErr("batchInsert arg is empty;dataList=%s", dataList);
			return rt;
		}
		rt = openDao();
		if(rt != Errno.OK) {
			return rt;
		}

		rt = m_dao.batchInsertReturnIdList(getTableName(), dataList, queueIdList, setDataListElementNull);
		return rt;
	}
	public int delete(ParamMatcher matcher) {
		return delete(matcher, null);
	}
	public int delete(ParamMatcher matcher, Ref<Integer> refRowCount) {
		int rt;
		if(matcher == null || matcher.isEmpty()){
			rt = Errno.ERROR;
			Log.logErr("delete arg is empty;matcher=%s", matcher);
			return rt;
		}
		rt = openDao();
		if(rt != Errno.OK) {
			return rt;
		}
		rt = m_dao.delete(getTableName(), matcher, refRowCount);
		return rt;
	}

	public int update(ParamUpdater updater,ParamMatcher matcher) {
		return update(updater, matcher, null);
	}

	public int update(ParamUpdater updater, ParamMatcher matcher, Ref<Integer> refRowCount) {
		int rt;
		if(matcher == null || matcher.isEmpty() || updater == null || updater.isEmpty()){
			rt = Errno.ERROR;
			Log.logErr("update arg is empty;updater=%s;matcher=%s",updater, matcher);
			return rt;
		}
		rt = openDao();
		if(rt != Errno.OK) {
			return rt;
		}
		rt = m_dao.update(getTableName(), updater, matcher, refRowCount);
		return rt;
	}
	public int batchUpdate( ParamUpdater updater, ParamMatcher matcher, FaiList<Param> dataList) {
		int rt;
		if(updater == null || updater.isEmpty() || matcher == null || matcher.isEmpty() || dataList == null || dataList.isEmpty()){
			rt = Errno.ERROR;
			Log.logErr("batchUpdate arg is empty;updater=%s;matcher=%s;dataList=%s;", updater, matcher, dataList);
			return rt;
		}
		rt = openDao();
		if(rt != Errno.OK) {
			return rt;
		}

		rt = m_dao.doBatchUpdate(getTableName(), updater, matcher, dataList, null, true);
		return rt;
	}
	public int selectFirst(SearchArg searchArg, Ref<Param> ref) {
		return selectFirst(searchArg, ref, null);
	}
	/**
	 * @param onlyNeedFieds 这些字段最好是索引，不然有性能问题
	 * @return
	 */
	public int selectFirst(SearchArg searchArg, Ref<Param> ref, String[] onlyNeedFieds) {
		int srcLimit = searchArg.limit;
		searchArg.limit = 1;
		Ref<FaiList<Param>> listRef = new Ref<>();
		int rt = select(searchArg, listRef, onlyNeedFieds);
		searchArg.limit = srcLimit;
		if(listRef.value == null){
			return rt;
		}
		if(listRef.value.isEmpty()){
			ref.value = new Param();
			return rt;
		}
		ref.value = listRef.value.get(0);
		return rt;
	}

	public int select(SearchArg searchArg, Ref<FaiList<Param>> listRef) {
		return select(searchArg, listRef, null);
	}

	/**
	 * @param onlyNeedFieds 这些字段最好是索引，不然有性能问题
	 * @return
	 */
	public int select(SearchArg searchArg, Ref<FaiList<Param>> listRef, String ... onlyNeedFieds){
		Dao.SelectArg sltArg = new Dao.SelectArg();
		sltArg.table = getTableName();
		if(onlyNeedFieds != null && onlyNeedFieds.length > 0){
			sltArg.field = Str.join(",",onlyNeedFieds);
		}
		sltArg.searchArg = searchArg;
		int rt = openDao();
		if(rt != Errno.OK) {
			return rt;
		}
		FaiList<Param> list = m_dao.select(sltArg);
		if(list == null) {
			rt = Errno.DAO_ERROR;
			Log.logErr(rt, "select db err;");
			return rt;
		}
		listRef.value = list;
		if(list.isEmpty()) {
			rt = Errno.NOT_FOUND;
			return rt;
		}
		return rt;
	}

	public int select(Dao.SelectArg sltArg, Ref<FaiList<Param>> listRef){
		sltArg.table = getTableName();
		int rt = openDao();
		if(rt != Errno.OK) {
			return rt;
		}
		FaiList<Param> list = m_dao.select(sltArg);
		if(list == null) {
			rt = Errno.DAO_ERROR;
			Log.logErr(rt, "select db err;");
			return rt;
		}
		listRef.value = list;
		if(list.isEmpty()) {
			rt = Errno.NOT_FOUND;
			return rt;
		}
		return rt;
	}

	public int selectCount(SearchArg searchArg, Ref<Integer> countRef){
		Dao.SelectArg sltArg = new Dao.SelectArg();
		sltArg.table = getTableName();
		sltArg.searchArg = searchArg;
		sltArg.field = "count(1) as count";
		int rt = openDao();
		if(rt != Errno.OK) {
			return rt;
		}
		Param countInfo = m_dao.selectFirst(sltArg);
		if(Str.isEmpty(countInfo)){
			rt = Errno.DAO_ERROR;
			Log.logErr(rt, "select db err;");
			return rt;
		}
		countRef.value = countInfo.getInt("count");
		return rt;
	}


	/**
	 * 获取daopool
	 */
	protected abstract DaoPool getDaoPool();
	/**
	 * 获取要操作的数据表
	 */
	protected abstract String getTableName();

	protected Dao getDao(){
		return m_dao;
	}
	protected String getGroup(){
		if(group == null){
			return defaultGroup;
		}
		return group;
	}
	public int getFlow() {
		return flow;
	}

	public int getAid() {
		return aid;
	}

	protected boolean m_daoOpened = false;
	protected Dao m_dao;
	private boolean useCommDao = false;
	protected int flow;
	protected int aid;
	protected String group;

	protected static String defaultGroup;
	protected static DaoProxy m_daoProxy;

	private static ConcurrentHashMap<Long, LinkedList<DaoCtrl>> openedDaoRecord = new ConcurrentHashMap<>();
}
