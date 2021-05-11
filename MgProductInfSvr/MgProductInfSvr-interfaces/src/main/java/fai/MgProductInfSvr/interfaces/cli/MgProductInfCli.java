package fai.MgProductInfSvr.interfaces.cli;

import fai.MgProductInfSvr.interfaces.cmd.MgProductInfCmd;
import fai.MgProductInfSvr.interfaces.dto.*;
import fai.MgProductInfSvr.interfaces.entity.*;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearch;
import fai.comm.netkit.FaiClient;
import fai.comm.netkit.FaiProtocol;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;

import java.nio.ByteBuffer;

// 对外统一提供的接口类,接口都在各个父类中
public class MgProductInfCli extends MgProductInfCli5ForProductScAndStore {
    public MgProductInfCli(int flow) {
        super(flow);
    }
}