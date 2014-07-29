package co.tomlee.guava.services.nifty;

import com.facebook.nifty.core.*;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.jboss.netty.channel.group.DefaultChannelGroup;

public class NiftyThriftServerService extends AbstractIdleService {
    public static final NettyServerConfig DEFAULT_NETTY_SERVER_CONFIG =
        new NettyServerConfigBuilder()
            .setBossThreadCount(8)
            .setWorkerThreadCount(256)
            .build();

    private final NettyServerConfig nettyServerConfig;
    private final ThriftServerDef thriftServerDef;

    private NettyServerTransport serverTransport;

    public NiftyThriftServerService(final TProcessor processor) {
        this(processor, 8080);
    }

    public NiftyThriftServerService(final TProcessor processor, final int port) {
        this(new ThriftServerDefBuilder()
                .withProcessor(processor)
                .protocol(new TCompactProtocol.Factory())
                .listen(port)
                .build());
    }

    public NiftyThriftServerService(final ThriftServerDef thriftServerDef) {
        this(DEFAULT_NETTY_SERVER_CONFIG, thriftServerDef);
    }

    public NiftyThriftServerService(final NettyServerConfig nettyServerConfig,
                                    final ThriftServerDef thriftServerDef) {
        this.nettyServerConfig = nettyServerConfig;
        this.thriftServerDef = thriftServerDef;
    }

    @Override
    protected void startUp() throws Exception {
        serverTransport = new NettyServerTransport(thriftServerDef,
                                                   nettyServerConfig,
                                                   new DefaultChannelGroup());
        serverTransport.start();
    }

    @Override
    protected void shutDown() throws Exception {
        serverTransport.stop();
    }
}
