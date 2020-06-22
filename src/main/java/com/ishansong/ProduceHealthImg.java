package com.ishansong;

import com.alibaba.fastjson.JSONObject;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Int;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ProduceHealthImg extends AbstractJavaSamplerClient {
    private static Producer<String, String> procuder;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "bigdata-dev-mq:9092,bigdata-dev-mq:9093,bigdata-dev-mq:9094");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        procuder = new KafkaProducer<String,String>(props);
    }

    public static void produce(Integer id,Integer courierId,String headUrl,String healthImgUrl){
        //生产者发送消息
        String topic = "courier_health_check_req";


        //准备写入topic的数据
        JSONObject params = new JSONObject();
        params.put("courierId",courierId);
        params.put("headUrl",headUrl);
        params.put("healthImgUrl",healthImgUrl);
        params.put("id",id);
        params.put("frontResult",true);
        params.put("cityId",1101);
        params.put("createTime",getFormatTime());//2020-01-02 01:12:12 getFormatTime()

        ProducerRecord<String, String> msg = new ProducerRecord<String,String>(topic,params.toJSONString());
        procuder.send(msg);
        procuder.flush();


    }



    public static void main(String[] args) {
        ProduceHealthImg producer = new ProduceHealthImg();
        producer.produce(210,2743999,"/head/2017/05/24/432a8dc7-f0b4-46e5-b8b3-a2415bd7f1a1.jpg","/shortPeriod/courier_aceExamine_storage/2020/06/22/73cef0ed-4b5a-4f0e-b58e-a745ca2bf8dc.jpg");

    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = new Arguments();
        params.addArgument("id","1");
        params.addArgument("courierId","15894156"); //闪送员id
        params.addArgument("headUrl","");//头像
        params.addArgument("healthImgUrl","");//健康宝
        return params;
    }
    @Override
    public SampleResult runTest(JavaSamplerContext arg0) {
        Integer id = Integer.valueOf(arg0.getParameter("id"));
        Integer courierId = Integer.valueOf(arg0.getParameter("courierId"));
        String headUrl = arg0.getParameter("headUrl");
        String healthImgUrl = arg0.getParameter("healthImgUrl");
        SampleResult sr = new SampleResult();
        sr.setSampleLabel( "Java请求");
        sr.sampleStart();// jmeter 开始统计响应时间标记
        ProduceHealthImg.produce(id,courierId,headUrl,healthImgUrl);
        sr.setSuccessful(true);
        sr.sampleEnd();
        return sr;
    }


    /**
     * 时间戳生成格式化的时间，如2019-09-04 03:05:34
     */
    public static String getFormatTime(){
        long time = System.currentTimeMillis();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = df.format(new Date(Long.valueOf(time)));
        return date;
    }

}
