package fai.MgProductSpecSvr.domain.repository;

import fai.comm.util.Dao;
import fai.comm.util.Errno;


public class TransacationCrtl {
    // 联合使用同一个
    public boolean registered(DaoCtrl daoCtrl){
        if(firstDaoCtrl == null){
            daoCtrl.openDao();
            firstDaoCtrl = daoCtrl;
            return true;
        }

        // 共同的daoProxy 才可以使用共同的dao
        if(!firstDaoCtrl.getDaoProxy().equals(daoCtrl.getDaoProxy())){
            return false;
        }
        Dao dao = firstDaoCtrl.getDao();
        return daoCtrl.openDao(dao) == Errno.OK;
    }
    public int setAutoCommit(boolean autoCommit){
        if(firstDaoCtrl == null){
            return Errno.ERROR;
        }
        return firstDaoCtrl.getDao().setAutoCommit(autoCommit);
    }
    public void commit(){
        if(firstDaoCtrl == null){
            return;
        }
        firstDaoCtrl.getDao().commit();
    }
    public void rollback(){
        if(firstDaoCtrl == null){
            return;
        }
        firstDaoCtrl.getDao().rollback();
    }
    public void closeDao(){
        if(firstDaoCtrl != null){
            firstDaoCtrl.closeDao();
        }
    }

    private DaoCtrl firstDaoCtrl = null;
}
