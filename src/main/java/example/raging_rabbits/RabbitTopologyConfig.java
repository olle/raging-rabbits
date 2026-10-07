package example.raging_rabbits;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(RagingClientsProperties.class)
public class RabbitTopologyConfig {

  // Spring Boot 4.x no longer auto-configures a RabbitAdmin: declare our own
  // so declared exchanges/queues/bindings are (re)declared on (re)connect.
  @Bean
  RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
    RabbitAdmin admin = new RabbitAdmin(connectionFactory);
    admin.setAutoStartup(true);
    return admin;
  }
}
