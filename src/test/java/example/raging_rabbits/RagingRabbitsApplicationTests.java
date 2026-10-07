package example.raging_rabbits;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "app.clients=5",
      "app.topics=orders,payments",
      "app.keys=1",
      "app.fanouts=test.broadcast",
      "app.noise=off",
      "app.concurrency=2",
      "app.log-every=5"
    })
class RagingRabbitsApplicationTests {

	@Test
	void contextLoads() {
	}

}
