package co.tomlee.guava.services.nifty;

import com.facebook.nifty.core.*;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.thrift.TProcessor;
import org.jboss.netty.channel.group.DefaultChannelGroup;

public class NiftyServerService extends AbstractIdleService {
    private final NiftyServerServiceConfiguration configuration;

    private NettyServerTransport serverTransport;

    public NiftyServerService(final TProcessor processor) {
        this(new NiftyServerServiceConfiguration(processor));
    }

    public NiftyServerService(final TProcessor processor, final int port) {
        this(new NiftyServerServiceConfiguration(processor).port(port));
    }

    public NiftyServerService(final NiftyServerServiceConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void startUp() throws Exception {
        serverTransport = new NettyServerTransport(configuration.thriftServerDef(),
                                                   configuration.nettyServerConfig(),
                                                   new DefaultChannelGroup());
        serverTransport.start();
    }

    @Override
    protected void shutDown() throws Exception {
        serverTransport.stop();
        serverTransport = null;
    }
}
