package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.executors.DefaultCommandExecutor;
import redis.clients.jedis.providers.SentineledConnectionProvider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class JedisSentineledPrefixedKeysTest extends PrefixedKeysTest {
    private final Set<HostAndPort> sentinels = new HashSet<>(Arrays.asList( HostAndPorts.getSentinelServers().get(1), HostAndPorts.getSentinelServers().get(3)));

    @Override
    UnifiedJedis prefixingJedis() {
        SentineledConnectionProvider connectionProvider = new SentineledConnectionProvider("mymaster", DefaultJedisClientConfig.builder().timeoutMillis(1000).password("foobared").database(2).build(), new GenericObjectPoolConfig<>(), sentinels, DefaultJedisClientConfig.builder().build());
        return new JedisSentineled(new DefaultCommandExecutor(connectionProvider), connectionProvider, new CommandObjectsWithPrefixedKeys("test-prefix:"), RedisProtocol.RESP3);
    }

    @Override
    UnifiedJedis nonPrefixingJedis() {
        SentineledConnectionProvider connectionProvider = new SentineledConnectionProvider("mymaster", DefaultJedisClientConfig.builder().timeoutMillis(1000).password("foobared").database(2).build(), new GenericObjectPoolConfig<>(), sentinels, DefaultJedisClientConfig.builder().build());
        return new JedisSentineled(new DefaultCommandExecutor(connectionProvider), connectionProvider, new CommandObjects(), RedisProtocol.RESP3);
    }
}
