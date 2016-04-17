package com.iecas.kds.tools.redis;

import redis.clients.jedis.*;

import java.util.*;

/**
 * Created by Administrator on 2016/3/29.
 */
public class RedisAPI {

    private static JedisPool pool = null;

    static {

        JedisPoolConfig config = new JedisPoolConfig();
        // 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
        // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        //config.setMaxTotal(Integer.parseInt(Properties_redis.application.getProperty("MaxTotal")));
        // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(Integer.parseInt(Properties_redis.application.getProperty("MaxIdle")));
        // 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        config.setMaxWaitMillis(Integer.parseInt(Properties_redis.application.getProperty("MaxWaitMillis")));
        // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        config.setTestOnBorrow(true);
        // 逐出连接的最小空闲时间，默认为1800000（30分钟）
        //config.setMinEvictableIdleTimeMillis(Integer.parseInt(Properties_redis.application.getProperty("MinEvictableIdleTimeMillis")));
        pool = new JedisPool(config, Properties_redis.application.getProperty("ip_redis"), Integer.parseInt(Properties_redis.application.getProperty("port_redis")),10000);

    }

    /**
     * 构建redis连接池
     *
     * @return JedisPool
     */
    public static JedisPool getPool() {
        return pool;
    }

    /**
     * 返还到连接池
     *
     * @param pool
     * @param redis
     */
    public static void returnResource(JedisPool pool, Jedis redis) {
        if (redis != null) {
            pool.returnResource(redis);
        }
    }

    /**
     * 通过指定模式，获取该模式对应的key
     * @param pattern
     * @return
     */
    public static synchronized Set<String> getKeysByPattern(String pattern){

        Set<String> keys = null;

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            keys = jedis.keys(pattern);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }
        return keys;
    }


    /**
     * 通过指定模式，获取该模式对应的key
     * @param pattern
     * @return
     */
    public static synchronized Map<String,String> getMapByPattern(String pattern){

        Map<String,String> map = new HashMap<>();

        JedisPool pool = null;
        Jedis jedis = null;
        Pipeline pipeline = null;
        try {

            pool = getPool();
            jedis = pool.getResource();
            pipeline = jedis.pipelined();

            Response<Set<String>> response_keys = pipeline.keys(pattern);
            //执行同步
            pipeline.sync();

            Set<String> keys = response_keys.get();
            Map<String,Response<String>> responseMap = new HashMap<>();

            for (String key : keys) {
                Response<String> response_value = pipeline.get(key);
                responseMap.put(key , response_value);
            }

            pipeline.sync();

            for (Map.Entry<String, Response<String>> entry : responseMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().get();
                map.put(key,value);
            }

            //执行同步
            pipeline.sync();
            //keys = jedis.keys(pattern);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return  map;

    }
    /**
     * 通过key，删除pair
     * @return
     */
    public static synchronized void deleteByKeyName(String key){

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            jedis.del(key);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }
    }

    /**
     * 通过listkey，删除pair
     * @return
     */
    public static synchronized void deleteByKeyList(List<String> keys){

        JedisPool pool = null;
        Jedis jedis = null;
        try {

            pool = getPool();
            jedis = pool.getResource();

            for (String key : keys) {
                jedis.del(key);
            }

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }
    }

    /**
     * 批量删除数据
     * @param keyList
     */
    public static synchronized void deleteByPipelined(List<String> keyList){

        JedisPool pool = null;
        Jedis jedis = null;
        Pipeline pipeline = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            pipeline = jedis.pipelined();

            for (String key : keyList) {
                pipeline.del(key);
            }

            //执行同步
            pipeline.sync();

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }

    }

    /**
     * 获取数据（key-value）
     *
     * @param key
     * @return
     */
    public static synchronized String get(String key) {
        String value = null;

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            value = jedis.get(key);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return value;
    }

    /**
     * 置入数据（key-value）
     *
     * @param key
     * @param value
     */
    public static synchronized void set(String key, String value) {

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            jedis.set(key, value);
        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }

    }

    /**
     * 批量插入数据(key-value)
     * @param map
     */
    public static synchronized void setByPipelined(Map<String,String> map){

        JedisPool pool = null;
        Jedis jedis = null;
        Pipeline pipeline = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            pipeline = jedis.pipelined();

            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                pipeline.set(key,value);
            }

            //执行同步
            pipeline.sync();

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }

    }

    /**
     * 批量插入数据(key-list)
     * @param map
     */
    public static synchronized void setListByPipelined(Map<String,List<String>> map){

        JedisPool pool = null;
        Jedis jedis = null;
        Pipeline pipeline = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            pipeline = jedis.pipelined();

            for (Map.Entry<String, List<String>> entry : map.entrySet()) {

                String key = entry.getKey();
                List<String> list = entry.getValue();

                for (String value : list) {
                    pipeline.rpush(key,value);
                }

            }

            //执行同步
            pipeline.sync();

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }

    }

    /**
     * 批量插入数据(key-map)
     * @param map
     */
    public static synchronized void setMapByPipelined(Map<String,Map<String,String>> map){

        JedisPool pool = null;
        Jedis jedis = null;
        Pipeline pipeline = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            pipeline = jedis.pipelined();

            for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {

                String key = entry.getKey();
                Map<String,String> value = entry.getValue();

                if (null!=value){
                    pipeline.hmset(key,value);
                }

            }

            //执行同步
            pipeline.sync();

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }

    }

    /**
     * 获取数据（key-list）
     *
     * @param key
     * @return
     */
    public static synchronized List<String> getList(String key) {

        List<String> values = new ArrayList<String>();

        JedisPool pool = null;
        Jedis jedis = null;
        try {

            pool = getPool();
            jedis = pool.getResource();
            values = jedis.lrange(key, 0, -1);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return values;

    }

    /**
     * 将数据一一置入key-list这样的缓存中
     * @param key
     * @param str
     */
    public static synchronized void setListOneByOne(String key,String str){

        JedisPool pool = null;
        Jedis jedis = null;

        try {
            pool = getPool();
            jedis = pool.getResource();

            jedis.rpush(key, str);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }

    }

    /**
     * 置入数据（key-list）
     *
     * @param key
     * @param list
     */
    public static synchronized void setList(String key, ArrayList<String> list) {

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            if (null != list) {
                // 循环置入数据
                for (String str : list) {
                    jedis.rpush(key, str);
                }
            }

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }
    }

    /**
     * 置入数据（key-set）
     *
     * @param key
     * @param set
     */
    public static synchronized void setSet(String key, Set<String> set) {

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            if (null != set) {

                for (String str : set) {
                    jedis.sadd(key, str);
                }

            }

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }

    }

    /**
     * 获取数据（key-set）
     *
     * @param key
     * @return
     */
    public static synchronized Set<String> getSet(String key) {

        Set<String> setValues = null;
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            setValues = jedis.smembers(key);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return setValues;

    }

/*
	*/
/**
 * 置入数据（key-sortedset）
 * @param key
 * @param set
 *//*

	public static synchronized void setSortedSet(String key, Set<SortedSet> set) {

		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			if (null != set) {

				for (SortedSet sortedSet : set) {
					jedis.zadd(key, sortedSet.getNum(), sortedSet.getValue());
				}

			}

		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			e.printStackTrace();
		} finally {
			jedis.save();
			// 返还到连接池
			returnResource(pool, jedis);
		}

	}

	*/
/**
 * 获取数据（key-sortedset）
 * @param key
 * @return
 *//*

	public static synchronized Set<String> getSortedSet(String key){
		Set<String> setValues = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			setValues = jedis.zrange(key, 0, -1);

		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}

		return setValues;
	}
*/

    /**
     * 设置数据（key-hash）
     */
    public static synchronized void setHash(String key,Map<String, String> pairs){

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            if (null!=pairs) {
                jedis.hmset(key, pairs);
            }

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            //jedis.save();
            // 返还到连接池
            returnResource(pool, jedis);
        }

    }

    /**
     * 获取数据（key-hash）
     * @param key
     * @return
     */
    public static synchronized List<String> getHash(String key,String[] strArray){
        List<String> values = null;
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            values = jedis.hmget(key, strArray);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return values;
    }

    /**
     * 获取数据（key-hash）:获取hash中的所有key值
     * @param key
     * @return
     */
    public static synchronized Set<String> getHash_hkey(String key){
        Set<String> values = null;
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            values = jedis.hkeys(key);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return values;
    }

    /**
     * 获取数据（key-hash）:获取hash中的所有value值
     * @param key
     * @return
     */
    public static synchronized List<String> getHash_hvals(String key){
        List<String> values = null;
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            values = jedis.hvals(key);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return values;
    }

    /**
     * 获取数据（key-hash）：获取所有hash（包括key和value）
     * @param key
     * @return
     */
    public static synchronized Map<String, String> getHash_getAll(String key){
        Map<String, String> pairs = null;
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            pairs = jedis.hgetAll(key);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return pairs;
    }


    /**
     * 分布式id生成器
     * @param key
     * @return
     */
    public static synchronized String incr(String key) {
        String value = null;

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();
            jedis.incr(key);

        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }

        return value;
    }
}
