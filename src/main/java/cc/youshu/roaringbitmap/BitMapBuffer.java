package cc.youshu.roaringbitmap;


import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author sunyf
 * @description
 * @date 2019-12-13
 **/
public class BitMapBuffer implements GenericUDAFEvaluator.AggregationBuffer {
    private static final Logger LOG = Logger.getLogger(BitMapBuffer.class);

    private MutableRoaringBitmap bitMap;

    public BitMapBuffer(){
        bitMap = new MutableRoaringBitmap();
    }

    public void addItem(int id) {
        bitMap.add(id);
    }

    public void merge(byte[] buffer) throws IOException{
        if (buffer == null) {
            return;
        }

        ImmutableRoaringBitmap other = new ImmutableRoaringBitmap(ByteBuffer.wrap(buffer));
        if (bitMap == null) {
            LOG.debug("bitMap is null; other size = "+other.getLongSizeInBytes()+" count = " + other.getLongCardinality());
            bitMap = other.toMutableRoaringBitmap();
        } else {
            bitMap.or( other.toMutableRoaringBitmap());
        }
    }

    public byte[] getPartial() throws IOException {
        if (bitMap == null) {
            return null;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bitMap.serialize(dos);
        dos.close();
        return bos.toByteArray();
    }

    public int  getCardinalityCount(){
        return bitMap.getCardinality();
    }

    public void reset() {
        bitMap.clear();
    }



}
