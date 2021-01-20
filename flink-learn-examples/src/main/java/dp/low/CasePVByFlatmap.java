package dp.low;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author wangwenbo
 * @Date 2020/12/9 11:14 下午
 * @Version 1.0
 */
public class CasePVByFlatmap {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据、转换成 bean对象
        env
                .readTextFile("/Users/wangwenbo/Data/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] datas = value.split(",");
                        if ("pv".equals(datas[3])) {
                            out.collect(Tuple2.of("pv", 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1)
                .print("pv by flatmap");


        env.execute();
    }

}
