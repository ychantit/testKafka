package com.worldpay.edp.tests.kafka;

public class TestKafka {

	public static void main(String[] args) {

		if(args.length!=4){
			System.out.println("TestKafka requires 3 args : brocker topic partition nbrMsg");
		}else{
			String url = args[0];
			String topic = args[1];
			int partition = Integer.parseInt(args[2]);
			int nbrMsg = Integer.parseInt(args[3]);
			KafkaUtils.consumerMsgs(url, topic, partition, nbrMsg);
		}
	}

}
