package co.tomlee.guava.services.nifty;

import com.facebook.nifty.core.NettyServerConfig;
import com.facebook.nifty.core.NettyServerConfigBuilder;
import com.facebook.nifty.core.ThriftServerDef;
import com.facebook.nifty.core.ThriftServerDefBuilder;
import io.airlift.units.Duration;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.concurrent.TimeUnit;

public final class NiftyServerServiceConfiguration {
    private ThriftServerDefBuilder serverDefBuilder = new ThriftServerDefBuilder()
                                                        .protocol(new TCompactProtocol.Factory(NiftyServiceDefaults.MAX_FRAME_SIZE))
                                                        .listen(NiftyServiceDefaults.PORT);

    private NettyServerConfigBuilder serverConfigBuilder = new NettyServerConfigBuilder()
                                                            .setBossThreadCount(4)
                                                            .setWorkerThreadCount(128);

    public NiftyServerServiceConfiguration(final TProcessor processor) {
        serverDefBuilder.withProcessor(processor);
    }

    public NiftyServerServiceConfiguration protocol(final TProtocolFactory protocol) {
        serverDefBuilder.protocol(protocol);
        return this;
    }

    public NiftyServerServiceConfiguration maxConnections(final int maxConnections) {
        serverDefBuilder.limitConnectionsTo(maxConnections);
        return this;
    }

    public NiftyServerServiceConfiguration maxFrameSize(final int maxFrameSize) {
        serverDefBuilder.limitFrameSizeTo(maxFrameSize);
        return this;
    }

    public NiftyServerServiceConfiguration maxQueuedResponsesPerConnection(final int maxQueuedResponses) {
        serverDefBuilder.limitQueuedResponsesPerConnection(maxQueuedResponses);
        return this;
    }

    public NiftyServerServiceConfiguration idleTimeout(final int idleTimeout, final TimeUnit idleTimeoutUnit) {
        serverDefBuilder.clientIdleTimeout(new Duration(idleTimeout, idleTimeoutUnit));
        return this;
    }

    public NiftyServerServiceConfiguration taskTimeout(final int taskTimeout, final TimeUnit taskTimeoutUnit) {
        serverDefBuilder.taskTimeout(new Duration(taskTimeout, taskTimeoutUnit));
        return this;
    }

    public NiftyServerServiceConfiguration port(final int port) {
        serverDefBuilder.listen(port);
        return this;
    }

    public NiftyServerServiceConfiguration bossThreads(final int bossThreads) {
        serverConfigBuilder.setBossThreadCount(bossThreads);
        return this;
    }

    public NiftyServerServiceConfiguration workerThreads(final int workerThreads) {
        serverConfigBuilder.setWorkerThreadCount(workerThreads);
        return this;
    }

    ThriftServerDef thriftServerDef() {
        return serverDefBuilder.build();
    }

    NettyServerConfig nettyServerConfig() {
        return serverConfigBuilder.build();
    }
}
