package fai.MgProductInfSvr.application;
import fai.comm.jnetkit.server.ServerConfig;
import fai.comm.jnetkit.server.fai.FaiServer;

public class MgProductInfSvr {
    public static void main(String[] args) throws Exception {
	ServerConfig config = new ServerConfig(args);
        FaiServer server = new FaiServer(config);

        server.setHandler(new MgProductInfHandler(server));
        server.start();
    }
}
