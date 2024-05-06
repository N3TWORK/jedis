package redis.clients.jedis;

final class MultiNodePipelineCommand {
    private final CommandObject<?> commandObject;
    private final Response<?> response;
    private HostAndPort nodeKey;
    private int attempt;

    public MultiNodePipelineCommand(CommandObject<?> commandObject, Response<?> response, HostAndPort nodeKey, int attempt) {
        this.commandObject = commandObject;
        this.response = response;
        this.nodeKey = nodeKey;
        this.attempt = attempt;
    }

    public CommandObject<?> getCommandObject() {
        return commandObject;
    }

    public Response<?> getResponse() {
        return response;
    }

    public HostAndPort getNodeKey() {
        return nodeKey;
    }

    public int getAttempt() {
        return attempt;
    }

    public void incrementAttempt() {
        ++attempt;
    }

    public void setNodeKey(HostAndPort nodeKey) {
        this.nodeKey = nodeKey;
    }
}
