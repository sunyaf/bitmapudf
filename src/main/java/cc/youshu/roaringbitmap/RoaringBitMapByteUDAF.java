package cc.youshu.roaringbitmap;

import cc.youshu.bitmap.BitMapBuffer;
import cc.youshu.bitmap.RoaringBitmapDistinctUDAF;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

/**
 * @author sunyf
 * @description gorup分组后存储到roaringbitmap里面
 * @date 2019-12-12
 **/
@Description(name = "bit_map_byte",
        value = "_FUNC_(x) - Returns  the distinct element  roaringbitmap byte")
public class RoaringBitMapByteUDAF extends AbstractGenericUDAFResolver {
    private static final Log LOG = LogFactory.getLog(RoaringBitmapDistinctUDAF.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] params) throws SemanticException {
        if (params.length > 1) {
            throw new UDFArgumentLengthException("Exactly one argument is expected.");
        }
        return new BitmapDistinctUDAFEvaluator();
    }

    public static class BitmapDistinctUDAFEvaluator extends GenericUDAFEvaluator {
        private LongObjectInspector intInputOI;

        private BinaryObjectInspector partialBufferOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // iterate() gets called.. string and int passed in
                this.intInputOI = (LongObjectInspector) parameters[0];
            } else {
                this.partialBufferOI = (BinaryObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;

        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            BitMapBuffer bitMapAgg = new BitMapBuffer();
            reset(bitMapAgg);
            return bitMapAgg;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            BitMapBuffer bitMapAgg = (BitMapBuffer) aggregationBuffer;
            bitMapAgg.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            BitMapBuffer myagg = (BitMapBuffer) agg;
            myagg.addItem(PrimitiveObjectInspectorUtils.getInt(parameters[0], intInputOI));
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LOG.debug("terminatePartial");
            BitMapBuffer myagg = (BitMapBuffer) agg;
            try {
                return myagg.getPartial();
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                BitMapBuffer myagg = (BitMapBuffer) agg;
                byte[] partialBuffer = this.partialBufferOI.getPrimitiveJavaObject(partial);
                try {
                    myagg.merge(partialBuffer);
                } catch (IOException e) {
                    throw new HiveException(e);
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            try {
                BitMapBuffer myagg = (BitMapBuffer) agg;
                return myagg.getPartial();
            } catch (Exception e) {
                throw new HiveException(e);
            }
        }
    }


}
