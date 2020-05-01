## Transport library

Library provides communication between applications running on different JVMs using interface method calls.  

Key features:  
- Sync & async method calls - type of communication is determined by client, not server
- One interface could have multiple server implementations - 
  client choose required by specifying target module.id
- Request-scoped timeout for both sync/async calls
- 3 protocols:
  - ZeroMQ
    - Unlimited message size
    - Low latency
    - Pure TCP connection
  - Kafka
    - Persistence (messages could be replayed)
    - High throughput
  - HTTP
    - Low latency
    - High throughput
- User could specify custom security provider (see example below)

## Latency

**X axis** - sliding time window  
**Y axis** - response time in ms  
Dashboard address is logged at startup like:
```
2020-05-01 20:19:00 INFO  AdminServer:112 - Transport console started at http://host.docker.internal:62842/admin
```
#### HTTP SYNC  
<img src="https://raw.githubusercontent.com/dredwardhyde/transport-library/master/http.PNG" width="900"/>  

#### ZMQ SYNC  
<img src="https://raw.githubusercontent.com/dredwardhyde/transport-library/master/zmq.PNG" width="900"/>  

#### KAFKA SYNC   
<img src="https://raw.githubusercontent.com/dredwardhyde/transport-library/master/kafka.PNG" width="900"/>  

#### HTTP ASYNC  
<img src="https://raw.githubusercontent.com/dredwardhyde/transport-library/master/http_async.PNG" width="900"/>  

#### ZMQ ASYNC  
<img src="https://raw.githubusercontent.com/dredwardhyde/transport-library/master/zmq_async.PNG" width="900"/>  

#### KAFKA ASYNC   
<img src="https://raw.githubusercontent.com/dredwardhyde/transport-library/master/kafka_async.PNG" width="900"/>  

## How to use

You create interface with ```@Api```annotation, for example:

```java
@Api
public interface PersonService {
    public static final String TEST = "TEST";
    public static void lol3() {
        System.out.println("lol3");
    }
    public int add(String name, String email, Address address);
    public Person get(Integer id);
    public void lol();
    public void lol2(String message);
    public String getName();
    public Person testError();
}
```

Server-side implementation:
```java

@ApiServer
public class PersonServiceImpl implements PersonService{
    // Methods
    // ...
    public void lol(){
        RequestContext.getSourceModuleId(); // client module.id available on server side
        RequestContext.getTicket(); // and security ticket too
    }
    public Person testError() {
        throw new RuntimeException("Exception in " + System.getProperty("module.id"));
    }
}
```

Then [transport-maven-plugin](https://github.com/dredwardhyde/transport-maven-plugin) generates client transport interface.  
This plugin ignores all the static and default methods, all fields:

```java
@ApiClient(ticketProvider = TicketProviderImpl.class)
public interface PersonServiceTransport {
    public Request<Integer> add(String name, String email, Address address);
    public Request<Person> get(Integer id);
    public Request<Void> lol();
    public Request<Void> lol2(String message);
    public Request<String> getName();
    public Request<Person> testError();
}
```

Security ticket provider could be specified by user:

```java
@Component
public class TicketProviderImpl implements TicketProvider {

    @Override
    public SecurityTicket getTicket() {
        // Specify user and security token
        return new SecurityTicket("user1", UUID.randomUUID().toString());
    }
}
```

Next, you could inject this transport interface using @Autowire:

```java
@Autowired
com.transport.test.PersonServiceTransport personService;

// Sync call on any implementation with 10s timeout:
Integer id = personService.add("Test name", "test@mail.com", null)
                          .withTimeout(TimeUnit.MILLISECONDS.toMillis(15000))
                          .onModule("test.server")
                          .executeSync();

// Async call on module with moduleId = main.server and timeout = 10s
personService.get(id)
             .onModule("main.server")
             .withTimeout(10_000)
             .executeAsync(UUID.randomUUID().toString(), PersonCallback.class);

// Async callback implementation example
public class PersonCallback implements Callback<Person> {

    // **key** - used as request ID, will be the same value that was used during invocation
    // **result** - result of method invocation
    // This method will be called if method was executed without exceptions
    // If T is Void then result will always be **null**
    @Override
    public void onSuccess(String key, Person result) {
        System.out.println("Key: " + key);
        System.out.println("Result: " + result);
    }

    // This method will be called if method has thrown exception OR execution timeout occurred
    @Override
    public void onError(String key, Throwable exception) {
        System.out.println("Exception during async call");
        exception.printStackTrace();
    }
}

```

## Configuration

```java
@Configuration
@ComponentScan
@Import(TransportConfig.class) // Import transport configuration
public class MainConfig {

    // Specify server implementation endpoints (must be empty if none exists)
    @Bean
    ServerEndpoints serverEndpoints(){ 
        return new ServerEndpoints(PersonServiceImpl.class, ClientServiceImpl.class); 
    }

    // Specify required client endpoints (must be empty if none exists)
    @Bean
    ClientEndpoints clientEndpoints(){ 
        return new ClientEndpoints(ClientServiceTransport.class, PersonServiceTransport.class); 
    }
}
```

NOTE: Number of partitions for library's topics is equal to broker's count.
      If any required topics already exist, but they have wrong configurations, exception will be thrown.

#### Available options
  **-Dzookeeper.connection**  - host:port for ZooKeeper cluster (required)  
  **-Dzmq.service.port**      - port for receiving request connections for ZeroMQ (optional, default port is 4242)  
  **-Dhttp.service.port**     - port for receiving request connections for HTTP (optional, default port is 4242)  
  **-Dzmq.callback.port**     - port for receiving callback connections for ZeroMQ (optional, default port is 4342)  
  **-Dhttp.callback.port**    - port for receiving callback connections for HTTP (optional, default port is 4342)  
  **-Dmodule.id**             - unique name of server in ZooKeeper cluster (required)  
  **-Dtransport.protocol**    - could be 'zmq', 'kafka' or 'http' (required)  
  **-Dbootstrap.servers**     - bootstrap servers of Kafka cluster  (optional, only when transport protocol is Kafka)  

