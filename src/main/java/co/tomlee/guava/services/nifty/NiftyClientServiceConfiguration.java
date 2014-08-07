package co.tomlee.guava.services.nifty;

import com.facebook.nifty.client.NettyClientConfig;
import com.facebook.nifty.client.NettyClientConfigBuilder;
import io.airlift.units.Duration;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.concurrent.TimeUnit;

public final class NiftyClientServiceConfiguration<T extends TServiceClient> {
    final Class<T> clientType;
    final TServiceClientFactory<T> clientFactory;
    NettyClientConfigBuilder clientConfigBuilder = new NettyClientConfigBuilder()
                                                        .setBossThreadCount(4)
                                                        .setWorkerThreadCount(128);

    TProtocolFactory protocolFactory = new TCompactProtocol.Factory(NiftyServiceDefaults.MAX_FRAME_SIZE);
    Duration connectTimeout = new Duration(2, TimeUnit.SECONDS);
    Duration receiveTimeout = new Duration(10, TimeUnit.SECONDS);
    Duration sendTimeout = new Duration(5, TimeUnit.SECONDS);
    int defaultPort = NiftyServiceDefaults.PORT;

    public NiftyClientServiceConfiguration(final Class<T> clientType, final TServiceClientFactory<T> clientFactory) {
        this.clientType = clientType;
        this.clientFactory = clientFactory;
    }

    public NiftyClientServiceConfiguration defaultPort(final int port) {
        this.defaultPort = port;
        return this;
    }

    public NiftyClientServiceConfiguration bossThreads(final int bossThreads) {
        clientConfigBuilder.setBossThreadCount(bossThreads);
        return this;
    }

    public NiftyClientServiceConfiguration workerThreads(final int workerThreads) {
        clientConfigBuilder.setWorkerThreadCount(workerThreads);
        return this;
    }

    public NiftyClientServiceConfiguration protocolFactory(final TProtocolFactory protocolFactory) {
        this.protocolFactory = protocolFactory;
        return this;
    }

    public NiftyClientServiceConfiguration connectTimeout(final int timeout, final TimeUnit unit) {
        connectTimeout = new Duration(timeout, unit);
        return this;
    }

    public NiftyClientServiceConfiguration receiveTimeout(final int timeout, final TimeUnit unit) {
        receiveTimeout = new Duration(timeout, unit);
        return this;
    }

    public NiftyClientServiceConfiguration sendTimeout(final int timeout, final TimeUnit unit) {
        sendTimeout = new Duration(timeout, unit);
        return this;
    }

    NettyClientConfig clientConfig() {
        return clientConfigBuilder.build();
    }
}
