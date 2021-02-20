package fai.MgProductSearchSvr.application;

import fai.comm.jnetkit.config.*;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.util.*;

public class MgProductSearchSvr {

    public static void main(String[] args) throws Exception {
        ServerConfig config = new ServerConfig(args);
        FaiServer server = new FaiServer(config);

        // get svr option
        SvrOption svrOption = server.getConfig().getConfigObject(SvrOption.class);
        boolean debug = svrOption.getDebug();
        Log.logStd("MgProductSearchSvr svrOption debug = %s;", debug);

        server.setHandler(new MgProductSearchHandler(server));
        server.start();
    }

    @ParamKeyMapping(path = ".svr")
    public static class SvrOption {
        private int lockLease = 1000;
        private boolean debug = false;
        private String productBasicDbInstance;
        private int dbMaxSize = 10;

        public int getDbMaxSize() {
            return dbMaxSize;
        }

        public void setDbMaxSize(int dbMaxSize) {
            this.dbMaxSize = dbMaxSize;
        }

        public int getLockLease() {
            return lockLease;
        }

        public void setLockLease(int lockLease) {
            this.lockLease = lockLease;
        }

        public boolean getDebug() {
            return debug;
        }

        public void setDebug(boolean debug) {
            this.debug = debug;
        }

        public String getProductBasicDbInstance() {
            return productBasicDbInstance;
        }

        public void setProductBasicDbInstance(String productBasicDbInstance) {
            this.productBasicDbInstance = productBasicDbInstance;
        }
    }
}
