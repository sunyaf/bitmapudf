package cc.youshu.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author sunyf
 * @description 从hbase读取bitmap二进制转化为一行一行的用户ID
 * @date 2020-01-02
 **/
public class BitMapUDTF extends GenericUDTF {

    private BinaryObjectInspector binaryOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException  {

        if (args.length != 1) {
            throw new UDFArgumentException("BitMapUDTF() takes exactly one argument");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
            throw new UDFArgumentException("BitMapUDTF() () takes a binary as a parameter");
        }

        // 输入格式（inspectors）
        binaryOI = (BinaryObjectInspector) args[0];

        // 输出格式（inspectors） -- 有两个属性的对象
        List<String> fieldNames = new ArrayList<String>(1);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
        fieldNames.add("id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

        @Override
    public void process(Object[] record) throws HiveException {
            byte[] idBytes = this.binaryOI.getPrimitiveJavaObject(record[0]);
            if(idBytes !=null && idBytes.length>0){
                ImmutableRoaringBitmap other = new ImmutableRoaringBitmap(ByteBuffer.wrap(idBytes));
                Iterator<Integer> iterator = other.iterator();
                while (iterator.hasNext()){
                    forward(new Object[] { iterator.next() });
                }
            }

        }


    @Override
    public void close() throws HiveException {

    }
}
