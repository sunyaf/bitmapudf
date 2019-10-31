package cc.youshu.redis;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import redis.clients.jedis.*;

import java.io.IOException;

/**
 * @author sunyf
 * @description udf to  redis set collection
 * @date 2019-10-30
 **/
@Description(name = "redis_hset",
        value = "_FUNC_(host_and_port,password,key,filed,value ) - Return ret "
)
public class RedisHSetUDF extends GenericUDF {

    private JedisPool jedisPool;
    private HostAndPort hostAndPort;
    private String key;
    private String password;

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        try (Jedis jedis = jedisPool.getResource()) {
            String field = arg0[3].get().toString();
            String value = arg0[4].get().toString();
            jedis.hset(key, field, value);
            return new IntWritable(1);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HiveException(e);
        }
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "redis_hset(redishost_and_port,password,key,field,value)";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        if (arg0.length != 5) {
            throw new UDFArgumentException(" Expecting arguments:<redishost:port> ,<password>, <key>, <field> ,<value> ");
        }
        //第一个参数校验
        if (arg0[0].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[0]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[0] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("redis host:port  must be constant");
            }
            ConstantObjectInspector redishost_and_port = (ConstantObjectInspector) arg0[0];

            String[] host_and_port = redishost_and_port.getWritableConstantValue().toString().split(":");
            hostAndPort = new HostAndPort(host_and_port[0], Integer.parseInt(host_and_port[1]));
        }

        //第2个参数校验
        if (arg0[1].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[1]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[1] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("redis password   must be constant");
            }
            ConstantObjectInspector keyFieldOI = (ConstantObjectInspector) arg0[1];

            password = keyFieldOI.getWritableConstantValue().toString();
        }

        //第3个参数校验
        if (arg0[2].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[2]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[2] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("redis hset key   must be constant");
            }
            ConstantObjectInspector keyFieldOI = (ConstantObjectInspector) arg0[2];

            key = keyFieldOI.getWritableConstantValue().toString();
        }

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(100);
        poolConfig.setMaxTotal(500);
        poolConfig.setMaxWaitMillis(10000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, hostAndPort.getHost(), hostAndPort.getPort(), 10000, password, 0);

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }


    @Override
    public void close() throws IOException {
        jedisPool.close();
    }


}
