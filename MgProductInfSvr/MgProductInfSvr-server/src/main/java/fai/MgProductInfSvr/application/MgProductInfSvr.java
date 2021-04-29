package fai.MgProductInfSvr.application;
import fai.comm.jnetkit.config.ParamKeyMapping;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;
import fai.comm.util.Log;

public class MgProductInfSvr {
    public static void main(String[] args) throws Exception {
	    ServerConfig config = new ServerConfig(args);
        FaiServer server = new FaiServer(config);

        SVR_OPTION = config.getConfigObject(SvrOption.class);
        Log.logStd("SVR_OPTION=%s", SVR_OPTION);

        server.setHandler(new MgProductInfHandler(server));
        server.start();
    }

    @ParamKeyMapping(path = ".svr")
    public static class SvrOption {
        /** 导入商品数据的最大批次 */
        private int importProductMaxSize = 100;

        public int getImportProductMaxSize() {
            return importProductMaxSize;
        }

        public void setImportProductMaxSize(int importProductMaxSize) {
            this.importProductMaxSize = importProductMaxSize;
        }

        @Override
        public String toString() {
            return "SvrOption{" +
                    "importProductMaxSize=" + importProductMaxSize +
                    '}';
        }
    }
    public static SvrOption SVR_OPTION;
}
