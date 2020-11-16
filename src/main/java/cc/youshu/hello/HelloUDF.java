package cc.youshu.hello;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * @author sunyf
 * @description udf 测试
 * @date 2019-11-18
 **/
public class HelloUDF extends UDF {



    @Description(
            name = "hello",
            value = "_FUNC_(str) - from the input string"
                    + "returns the value that is \"Hello $str\" ",
            extended = "Example:\n"
                    + " > SELECT _FUNC_(str) FROM src;"
    )
    public String evaluate(String[] str) throws HiveException {
        String b="";
        try {
            for(String a:str){
                b=b+a;
            }
            return "Hello " + b;
        } catch (Exception e) {
            throw new UDFArgumentException("hello error");
        }
    }

}
