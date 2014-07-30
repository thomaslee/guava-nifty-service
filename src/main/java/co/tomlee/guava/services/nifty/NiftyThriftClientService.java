package co.tomlee.guava.services.nifty;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.client.NettyClientConfig;
import com.facebook.nifty.client.NettyClientConfigBuilder;
import com.facebook.nifty.client.NiftyClient;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;

import java.net.InetSocketAddress;

public class NiftyThriftClientService<T extends TServiceClient> extends AbstractIdleService {
    private NiftyClient niftyClient;

    private final Class<? extends T> type;
    private final NettyClientConfig nettyClientConfig;
    private final TServiceClientFactory<T> serviceClientFactory;
    private final TProtocolFactory protocolFactory;

    public NiftyThriftClientService(final Class<? extends T> type,
                                    final TServiceClientFactory<T> serviceClientFactory) {
        this(type, serviceClientFactory, new TCompactProtocol.Factory(), new NettyClientConfigBuilder().build());
    }

    public NiftyThriftClientService(final Class<? extends T> type,
                                    final TServiceClientFactory<T> serviceClientFactory,
                                    final TProtocolFactory protocolFactory,
                                    final NettyClientConfig nettyClientConfig) {
        this.type = type;
        this.nettyClientConfig = nettyClientConfig;
        this.serviceClientFactory = serviceClientFactory;
        this.protocolFactory = protocolFactory;
    }

    @Override
    protected void startUp() throws Exception {
        niftyClient = new NiftyClient(nettyClientConfig);
    }

    @Override
    protected void shutDown() throws Exception {
        niftyClient.close();
    }

    public T newClient(final InetSocketAddress inetSocketAddress) throws TTransportException, InterruptedException {
        final FramedClientConnector framedClientConnector = new FramedClientConnector(inetSocketAddress);
        final TProtocol protocol = protocolFactory.getProtocol(niftyClient.connectSync(type, framedClientConnector));
        return serviceClientFactory.getClient(protocol);
    }

    public T newClient(final HostAndPort hostAndPort) throws TTransportException, InterruptedException {
        final FramedClientConnector framedClientConnector = new FramedClientConnector(hostAndPort);
        final TProtocol protocol = protocolFactory.getProtocol(niftyClient.connectSync(type, framedClientConnector));
        return serviceClientFactory.getClient(protocol);
    }

    public T newClient(final String host, final int port) throws TTransportException, InterruptedException {
        return newClient(HostAndPort.fromParts(host, port));
    }
}
