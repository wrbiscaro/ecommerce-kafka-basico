package br.com.kafka.ecommerce;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		//Cria um producer com as properties definidas no metodo properties()
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties());
		//Cria uma mensagem com usuario, pedido e valor, por exemplo
		String value = "usuario1,pedido1,valor1000";
		//Cria um novo registro, informando topico, chave e valor. Chave e valor serao o mesmo, nesse exemplo.
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);
		
		//Envia o registro para o topico. O metodo send() é assincrono (retorna um Future). 
		//Utilizamos o metodo get() para torna-lo sincrono, aguardando a producao para ver seu conteudo no callback (cuidado ao utilizar).
		producer.send(record, (data, ex) -> {
			//Como o send nao retorna detalhes, criamos essa callback para visualizacao dos detalhes da producao
			if (ex != null) {
				ex.printStackTrace(); //Ocorreu uma exception no producer, printa ela
				return;
			}
			System.out.println(data.topic() + ": " + " /partition " + data.partition() + " /offset " + data.offset() 
			+ " /timestamp " + data.timestamp()); //Producer foi realizado, exibe detalhes
		}).get();
	}

	private static Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //local onde o cluster kafka esta executando
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //serializa a chave string para bytes
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());	//serializa o valor string para bytes	
		return properties;
	}
}
