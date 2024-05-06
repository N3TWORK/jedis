package redis.clients.jedis;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.providers.ClusterConnectionProvider;
import redis.clients.jedis.util.IOUtils;

public class ClusterPipeline extends MultiNodePipelineBase {
  private static final int DEFAULT_MAX_ATTEMPTS = 5;
  public static final int DEFAULT_TIMEOUT = 2000;

  private final ClusterConnectionProvider provider;
  private final int maxAttempts;
  private final Instant deadline;
  private AutoCloseable closeable = null;

  public ClusterPipeline(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig, int maxAttempts, Duration maxTotalRetriesDuration) {
    this(new ClusterConnectionProvider(clusterNodes, clientConfig),
        createClusterCommandObjects(clientConfig.getRedisProtocol()), maxAttempts, maxTotalRetriesDuration);
    this.closeable = this.provider;
  }

  ClusterPipeline(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig) {
    this(clusterNodes, clientConfig, DEFAULT_MAX_ATTEMPTS, Duration.ofMillis((long) DEFAULT_MAX_ATTEMPTS * clientConfig.getSocketTimeoutMillis()));
  }

  public ClusterPipeline(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig,
      GenericObjectPoolConfig<Connection> poolConfig, int maxAttempts, Duration maxTotalRetriesDuration) {
    this(new ClusterConnectionProvider(clusterNodes, clientConfig, poolConfig),
        createClusterCommandObjects(clientConfig.getRedisProtocol()), maxAttempts, maxTotalRetriesDuration);
    this.closeable = this.provider;
  }

  ClusterPipeline(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig, GenericObjectPoolConfig<Connection> poolConfig) {
    this(new ClusterConnectionProvider(clusterNodes, clientConfig, poolConfig),
            createClusterCommandObjects(clientConfig.getRedisProtocol()), DEFAULT_MAX_ATTEMPTS, Duration.ofMillis((long) DEFAULT_MAX_ATTEMPTS * clientConfig.getSocketTimeoutMillis()));
    this.closeable = this.provider;
  }

  public ClusterPipeline(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig,
      GenericObjectPoolConfig<Connection> poolConfig, Duration topologyRefreshPeriod, int maxAttempts, Duration maxTotalRetriesDuration) {
    this(new ClusterConnectionProvider(clusterNodes, clientConfig, poolConfig, topologyRefreshPeriod),
        createClusterCommandObjects(clientConfig.getRedisProtocol()), maxAttempts, maxTotalRetriesDuration);
    this.closeable = this.provider;
  }

  public ClusterPipeline(ClusterConnectionProvider provider, int maxAttempts, Duration maxTotalRetriesDuration) {
    this(provider, new ClusterCommandObjects(), maxAttempts, maxTotalRetriesDuration);
  }

  ClusterPipeline(ClusterConnectionProvider provider) {
    this(provider, new ClusterCommandObjects(), DEFAULT_MAX_ATTEMPTS, Duration.ofMillis((long) DEFAULT_MAX_ATTEMPTS * DEFAULT_TIMEOUT));
  }

  ClusterPipeline(ClusterConnectionProvider provider, ClusterCommandObjects commandObjects) {
    this(provider, commandObjects, DEFAULT_MAX_ATTEMPTS, Duration.ofMillis((long) DEFAULT_MAX_ATTEMPTS * DEFAULT_TIMEOUT));
  }

  public ClusterPipeline(ClusterConnectionProvider provider, ClusterCommandObjects commandObjects, int maxAttempts, Duration maxTotalRetriesDuration) {
    super(commandObjects);
    this.provider = provider;
    this.maxAttempts = maxAttempts;
    deadline = Instant.now().plusMillis(maxTotalRetriesDuration.toMillis());
  }

  private static ClusterCommandObjects createClusterCommandObjects(RedisProtocol protocol) {
    ClusterCommandObjects cco = new ClusterCommandObjects();
    if (protocol == RedisProtocol.RESP3) cco.setProtocol(protocol);
    return cco;
  }

  @Override
  protected void handleExceptionOnSync(MultiNodePipelineCommand pipelineCommand,
                                          Connection connection,
                                          JedisException exception) {
    if (exception instanceof JedisClusterOperationException) {
      throw exception;
    }

    if (exception instanceof JedisMovedDataException) {
      provider.renewSlotCache(connection);
      HostAndPort newNodeKey = getNodeKey(pipelineCommand.getCommandObject().getArguments());
      pipelineCommand.setNodeKey(newNodeKey);
      requeueCommand(pipelineCommand, false);
    } else if (exception instanceof JedisAskDataException) {
      JedisAskDataException redirect = (JedisAskDataException) exception;
      pipelineCommand.setNodeKey(redirect.getTargetNode());
      requeueCommand(pipelineCommand, true);
    } else if (exception instanceof JedisConnectionException) {
      int attemptsLeft = maxAttempts - pipelineCommand.getAttempt() - 1;
      sleep(getBackoffSleepMillis(attemptsLeft, deadline));
      provider.renewSlotCache();
      requeueCommand(pipelineCommand, false);
    }
  }

  private void requeueCommand(MultiNodePipelineCommand pipelineCommand, boolean ask) {
    if (Instant.now().isAfter(deadline)) {
      throw new JedisClusterOperationException("Cluster retry deadline exceeded.");
    }

    if (pipelineCommand.getAttempt() >= maxAttempts) {
      throw new JedisClusterOperationException("No more cluster attempts left.");
    }

    Connection connection = connections.computeIfAbsent(pipelineCommand.getNodeKey(), this::getConnection);

    if (ask) {
      connection.sendCommand(Protocol.Command.ASKING);
    }

    pipelineCommand.incrementAttempt();
    pipelinedCommands.add(pipelineCommand);
    connection.sendCommand(pipelineCommand.getCommandObject().getArguments());
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      IOUtils.closeQuietly(closeable);
    }
  }

  @Override
  protected HostAndPort getNodeKey(CommandArguments args) {
    return provider.getNode(((ClusterCommandArguments) args).getCommandHashSlot());
  }

  @Override
  protected Connection getConnection(HostAndPort nodeKey) {
    return provider.getConnection(nodeKey);
  }

  private static long getBackoffSleepMillis(int attemptsLeft, Instant deadline) {
    if (attemptsLeft <= 0) {
      return 0;
    }

    long millisLeft = Duration.between(Instant.now(), deadline).toMillis();
    if (millisLeft < 0) {
      throw new JedisClusterOperationException("Cluster retry deadline exceeded.");
    }

    long maxBackOff = millisLeft / ((long) attemptsLeft * attemptsLeft);
    return ThreadLocalRandom.current().nextLong(maxBackOff + 1);
  }

  private void sleep(long sleepMillis) {
    if (sleepMillis <= 0) {
      return;
    }

    try {
      TimeUnit.MILLISECONDS.sleep(sleepMillis);
    } catch (InterruptedException e) {
      throw new JedisClusterOperationException(e);
    }
  }

  /**
   * This method must be called after constructor, if graph commands are going to be used.
   */
  public void prepareGraphCommands() {
    super.prepareGraphCommands(provider);
  }
}
