package co.tomlee.guava.services.nifty;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.client.NettyClientConfig;
import com.facebook.nifty.client.NettyClientConfigBuilder;
import com.facebook.nifty.client.NiftyClient;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;

import java.net.InetSocketAddress;

public class NiftyClientService<T extends TServiceClient> extends AbstractIdleService {
    private NiftyClient niftyClient;

    private final NiftyClientServiceConfiguration<T> configuration;

    public NiftyClientService(final Class<T> clientType, final TServiceClientFactory<T> clientFactory) {
        this(new NiftyClientServiceConfiguration<>(clientType, clientFactory));
    }

    public NiftyClientService(final NiftyClientServiceConfiguration<T> configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void startUp() throws Exception {
        niftyClient = new NiftyClient(configuration.nettyConfig);
    }

    @Override
    protected void shutDown() throws Exception {
        niftyClient.close();
        niftyClient = null;
    }

    public T newClient(final InetSocketAddress inetSocketAddress) throws TTransportException, InterruptedException {
        final TDuplexProtocolFactory duplexProtocolFactory = TDuplexProtocolFactory.fromSingleFactory(configuration.protocolFactory);
        final FramedClientConnector framedClientConnector = new FramedClientConnector(inetSocketAddress, duplexProtocolFactory);
        final TProtocol protocol = configuration.protocolFactory.getProtocol(niftyClient.connectSync(configuration.clientType, framedClientConnector));
        return configuration.clientFactory.getClient(protocol);
    }

    public T newClient(final HostAndPort hostAndPort) throws TTransportException, InterruptedException {
        return newClient(new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort()));
    }

    public T newClient(final String host, final int port) throws TTransportException, InterruptedException {
        return newClient(new InetSocketAddress(host, port));
    }

    public T newClient(final String host) throws TTransportException, InterruptedException {
        return newClient(host, configuration.defaultPort);
    }
}
