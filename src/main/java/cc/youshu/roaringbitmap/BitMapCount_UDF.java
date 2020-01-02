package cc.youshu.roaringbitmap;

import jodd.util.StringUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author sunyf
 * @description 读取bitMap的二进制，返回数量
 * @date 2020-01-02
 **/
@Description(name = "BitMapCount",
        value = "byte to bitmap and return count"
)
public class BitMapCount_UDF extends UDF {
    private static final Logger LOG = Logger.getLogger(BitMapCount_UDF.class);
    public Integer evaluate( BytesWritable value) {
        if (value == null ) {
            LOG.info(" value length is 0");
            return 0;
        }
        byte[] bytes = value.getBytes();
        ImmutableRoaringBitmap other = new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes));
        return other.getCardinality();
        }
    }
