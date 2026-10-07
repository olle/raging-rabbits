package example.raging_rabbits;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "app.clients=5",
      "app.topic-bindings=2",
      "app.fanout-exchanges=test.broadcast",
      "app.concurrency=2",
      "app.log-every=5"
    })
class RagingRabbitsApplicationTests {

	@Test
	void contextLoads() {
	}

}
