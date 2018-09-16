package utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtil {
  // 配置Redis连接器
  val host = "192.168.0.113"
  val port = 6379
  val timeout = 30000
  val config = new JedisPoolConfig
  config.setMaxTotal(200)
  config.setMaxIdle(50)
  config.setMinIdle(0)
  config.setMaxWaitMillis(10000)
  config.setTestOnBorrow(true)
  config.setTestOnReturn(true)
  config.setTestOnCreate(true)
  config.setTestWhileIdle(true)
  config.setTimeBetweenEvictionRunsMillis(30000)
  config.setNumTestsPerEvictionRun(10)
  config.setMinEvictableIdleTimeMillis(60000)

  // 连接池
  lazy val pool = new JedisPool(config, host, port, timeout)

  // 创建程序奔溃时，回收资源的线程
  lazy val hook = new Thread {
    override def run() = pool.destroy()
  }
  sys.addShutdownHook(hook)

}
