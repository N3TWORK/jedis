package redis.clients.jedis;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.graph.GraphCommandObjects;
import redis.clients.jedis.providers.ConnectionProvider;
import redis.clients.jedis.util.IOUtils;

public abstract class MultiNodePipelineBase extends PipelineBase {
  private final Logger log = LoggerFactory.getLogger(getClass());

  protected final Queue<MultiNodePipelineCommand> pipelinedCommands;
  protected final Map<HostAndPort, Connection> connections;
  private volatile boolean syncing = false;

  public MultiNodePipelineBase(CommandObjects commandObjects) {
    super(commandObjects);
    pipelinedCommands = new LinkedList<>();
    connections = new LinkedHashMap<>();
  }

  /**
   * Sub-classes must call this method, if graph commands are going to be used.
   * @param connectionProvider connection provider
   */
  protected final void prepareGraphCommands(ConnectionProvider connectionProvider) {
    GraphCommandObjects graphCommandObjects = new GraphCommandObjects(connectionProvider);
    graphCommandObjects.setBaseCommandArgumentsCreator((comm) -> this.commandObjects.commandArguments(comm));
    super.setGraphCommands(graphCommandObjects);
  }

  protected abstract HostAndPort getNodeKey(CommandArguments args);

  protected abstract Connection getConnection(HostAndPort nodeKey);

  @Override
  protected final <T> Response<T> appendCommand(CommandObject<T> commandObject) {
    HostAndPort nodeKey = getNodeKey(commandObject.getArguments());
    Response<T> response = new Response<>(commandObject.getBuilder());
    pipelinedCommands.add(new MultiNodePipelineCommand(commandObject, response, nodeKey, 0));
    Connection connection = connections.computeIfAbsent(nodeKey, this::getConnection);
    connection.sendCommand(commandObject.getArguments());
    return response;
  }

  @Override
  public void close() {
    try {
      sync();
    } finally {
      connections.values().forEach(IOUtils::closeQuietly);
    }
  }

  @Override
  public final void sync() {
    if (syncing) {
      return;
    }
    syncing = true;

    while (!pipelinedCommands.isEmpty()) {
      MultiNodePipelineCommand pipelineCommand = pipelinedCommands.poll();
      Connection connection = connections.get(pipelineCommand.getNodeKey());

      if (connection == null) {
        log.warn("Dropping pipeline command due to dropped connection to: {}", pipelineCommand.getNodeKey());
      } else {
        try {
          pipelineCommand.getResponse().set(connection.getOne());
        } catch (JedisException exception) {
          if (exception instanceof JedisDataException) {
            pipelineCommand.getResponse().set(exception);
          }

          handleExceptionOnSync(
                  pipelineCommand,
                  connection,
                  exception
          );
        }
      }
    }

    syncing = false;
  }

  protected abstract void handleExceptionOnSync(MultiNodePipelineCommand pipelineCommand,
                                                   Connection connection,
                                                   JedisException exception);

  @Deprecated
  public Response<Long> waitReplicas(int replicas, long timeout) {
    return appendCommand(commandObjects.waitReplicas(replicas, timeout));
  }
}
