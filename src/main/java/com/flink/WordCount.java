package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws  Exception{
        final StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<String> text = env.socketTextStream("beifeng04.com", 9999, "\n");

        SingleOutputStreamOperator<Word> sum = text.flatMap(new FlatMapFunction<String, Word>() {
            @Override
            public void flatMap(String s, Collector<Word> collector) throws Exception {
                String[] split = s.split("\\s");
                for (String str : split) {
                    collector.collect(new Word(str, 1));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");
        sum.print().setParallelism(1);
        env.execute("WordCount");

    }

    public  static  class Word{
        public  String word ;
        public  long count ;

        public Word(){

        };

        public Word(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "Word{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
