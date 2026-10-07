package example.raging_rabbits;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "app.clients=5",
      "app.topic-contexts=orders,payments",
      "app.topic-keys=1",
      "app.fanout-exchanges=test.broadcast",
      "app.concurrency=2",
      "app.log-every=5"
    })
class RagingRabbitsApplicationTests {

	@Test
	void contextLoads() {
	}

}
