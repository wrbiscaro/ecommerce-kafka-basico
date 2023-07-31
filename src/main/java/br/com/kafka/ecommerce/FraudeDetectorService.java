package br.com.kafka.ecommerce;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FraudeDetectorService {

	public static void main(String[] args) {
		//Cria um consumer com as properties definidas no metodo properties()
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties());
		//Informa a lista de topicos que o consumer ira se inscrever
		//Raramente devemos utilizar mais de um topico em um consumer, para evitar complexidade
		consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
		
		while(true) { //É uma boa pratica o consumer permanecer sempre rodando. Podemos adicionar uma logica para interrompe-lo.
			//O consumer ira checar o topico por 100 milisegundos, recebendo uma lista de registros
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			if(!records.isEmpty()) {
				System.out.println(records.count() + " records found!");
				for(ConsumerRecord<String, String> record: records) {
					System.out.println("-------------------------------------------");
					System.out.println("Processing new order, checking for fraud...");
					System.out.println(record.key());
					System.out.println(record.value());
					System.out.println(record.partition());
					System.out.println(record.offset());
					
					try { //Adicionamos um sleep apenas para simular o tempo de um processamento de fraude
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						//Ignorar, nunca ocorrera exception
						e.printStackTrace();
					}
					
					System.out.println("Order processed!");
				}
			}
		}
	}

	private static Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //local onde o cluster kafka esta executando
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //deserializa a chave em bytes para string
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //deserializa o valor em bytes para string
		//Cada servico de consumer deve pertencer a um grupo unico para receber todas as mensagem
		//Caso dois servicos estejam em uma mesmo grupo, as mensagens serao divididas entre eles
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudeDetectorService.class.getSimpleName()); //id do grupo
		return properties;
	}
}
