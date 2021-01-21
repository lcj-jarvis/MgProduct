package fai.MgProductStoreSvr.domain.repository;

import fai.comm.jnetkit.server.ServerConfig;

public class DaoProxy extends fai.comm.middleground.repository.DaoProxy {
    public DaoProxy(ServerConfig config) {
        super(config);
    }
}
