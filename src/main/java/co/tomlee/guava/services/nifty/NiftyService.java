package co.tomlee.guava.services.nifty;

import com.facebook.nifty.core.*;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.thrift.TProcessor;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.util.HashedWheelTimer;

public class NiftyService extends AbstractIdleService {
    public static final int DEFAULT_BOSS_THREAD_COUNT = 8;
    public static final int DEFAULT_WORKER_THREAD_COUNT = 256;

    private final TProcessor processor;

    private final int bossThreadCount;
    private final int workerThreadCount;

    private NettyServerTransport serverTransport;

    public NiftyService(final TProcessor processor) {
        this(processor, DEFAULT_BOSS_THREAD_COUNT, DEFAULT_WORKER_THREAD_COUNT);
    }

    public NiftyService(final TProcessor processor,
                        final int bossThreadCount,
                        final int workerThreadCount) {
        this.processor = processor;
        this.bossThreadCount = bossThreadCount;
        this.workerThreadCount = workerThreadCount;
    }

    @Override
    protected void startUp() throws Exception {
        final ThriftServerDef serverDef = new ThriftServerDefBuilder().withProcessor(processor).build();

        serverTransport = new NettyServerTransport(serverDef,
                                                   new NettyServerConfigBuilder()
                                                       .setBossThreadCount(bossThreadCount)
                                                       .setWorkerThreadCount(workerThreadCount)
                                                       .setTimer(new HashedWheelTimer())
                                                       .build(),
                                                   new DefaultChannelGroup());
        serverTransport.start();
    }

    @Override
    protected void shutDown() throws Exception {
        serverTransport.stop();
    }
}
