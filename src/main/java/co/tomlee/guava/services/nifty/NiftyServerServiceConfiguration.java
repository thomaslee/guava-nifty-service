package co.tomlee.guava.services.nifty;

import com.facebook.nifty.core.NettyServerConfig;
import com.facebook.nifty.core.NettyServerConfigBuilder;
import com.facebook.nifty.core.ThriftServerDef;
import com.facebook.nifty.core.ThriftServerDefBuilder;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;

public final class NiftyServerServiceConfiguration {
    private ThriftServerDefBuilder serverDefBuilder = new ThriftServerDefBuilder()
                                                        .protocol(new TCompactProtocol.Factory(NiftyServiceDefaults.MAX_FRAME_SIZE))
                                                        .listen(3210);

    private NettyServerConfigBuilder serverConfigBuilder = new NettyServerConfigBuilder()
                                                            .setBossThreadCount(4)
                                                            .setWorkerThreadCount(128);

    public NiftyServerServiceConfiguration(final TProcessor processor) {
        serverDefBuilder.withProcessor(processor);
    }

    public NiftyServerServiceConfiguration port(final int port) {
        serverDefBuilder.listen(port);
        return this;
    }

    ThriftServerDef thriftServerDef() {
        return serverDefBuilder.build();
    }

    NettyServerConfig nettyServerConfig() {
        return serverConfigBuilder.build();
    }
}
