package cc.youshu.roaringbitmap; /**
 * Copyright 2012 Klout, Inc
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import jodd.util.StringUtil;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Simple UDF for doing single PUT into HBase table.
 * NOTE: Not intended for doing massive reads from HBase, but only when relatively few rows are being read.
 */
@Description(name = "hbase_put",
        value = "string _FUNC_(config, map<string, string> key_value) - \n" +
                "string _FUNC_(config, key, value) - Do a HBase Put on a table. " +
                " Config must contain zookeeper \n" +
                "quorum, table name, column, and qualifier. Example of usage: \n" +
                "  hbase_put(map('hbase.zookeeper.quorum', 'hb-zoo1,hb-zoo2', \n" +
                "                'table_name', 'metrics', \n" +
                "                'family', 'c', \n" +
                "                'qualifier', 'q'), \n" +
                "            'test.prod.visits.total', \n" +
                "            '123456') "
)
public class HbasePut_UDF extends UDF {
    private static final Logger LOG = Logger.getLogger(HbasePut_UDF.class);


    public Integer evaluate(Map<String, String> configMap, String key, BytesWritable value) {
        if (value == null) {
            LOG.info(" value length is 0");
            return 0;
        }
        if (StringUtil.isBlank(key)) {
            LOG.info("key  length is 0");
            return 0;
        }
        byte[] bytes = value.getBytes();
        HTableFactory.checkConfig(configMap);

        try {
            if (bytes != null && bytes.length > 0) {
                Table table = HTableFactory.getHTable(configMap);
                Put thePut = new Put(key.getBytes());
                thePut.addColumn(configMap.get(HTableFactory.FAMILY_TAG).getBytes(), configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(), bytes);
                table.put(thePut);
                table.close();
                return 1;
            }else {
                return 0;
            }
        } catch (Exception exc) {
            LOG.error("Error while doing HBase Puts");
            throw new RuntimeException(exc);
        } finally {
            //HTableFactory.close();
        }
    }

    public String evaluate(Map<String, String> configMap, Map<String, String> keyValueMap) {
        HTableFactory.checkConfig(configMap);

        try {
            List<Put> putList = new ArrayList<Put>();
            for (Map.Entry<String, String> keyValue : keyValueMap.entrySet()) {
                Put thePut = new Put(keyValue.getKey().getBytes());
                thePut.add(configMap.get(HTableFactory.FAMILY_TAG).getBytes(),
                        configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(),
                        keyValue.getValue().getBytes());
                putList.add(thePut);
            }

            Table table = HTableFactory.getHTable(configMap);
            table.put(putList);
            return "Put " + keyValueMap.toString();
        } catch (Exception exc) {
            LOG.error("Error while doing HBase Puts");
            throw new RuntimeException(exc);
        }
    }

    public String evaluate(Map<String, String> configMap, String key, String value) {
        HTableFactory.checkConfig(configMap);

        try {
            Table table = HTableFactory.getHTable(configMap);
            Put thePut = new Put(key.getBytes());
            thePut.add(configMap.get(HTableFactory.FAMILY_TAG).getBytes(), configMap.get(HTableFactory.QUALIFIER_TAG).getBytes(), value.getBytes());
            table.put(thePut);
            return "Put " + key + ":" + value;
        } catch (Exception exc) {
            LOG.error("Error while doing HBase Puts");
            throw new RuntimeException(exc);
        }
    }
}
