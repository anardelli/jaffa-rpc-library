### Transport library

This library was created to provide communication between applications running on different JVMs
through interface method calls. 
Key features:
- Very easy to use
- Sync & async method calls - type determined by client, not server
- One interface could have multiple server implementations - 
  client choose the right one by specifying target module.id
- User-provided timeout for both sync/async calls
- High throughput with Kafka and low latency with ZMQ
- User could specify custom security provider (see example below)
 
### How it works for end user

You create interface with ```@Api```annotation, for example:

```java
@Api
public interface PersonService {
    public static final String TEST = "TEST"; // will be ignored
    public int add(String name,  String email, Address address);
    public Person get(Integer id);
    public void lol();
    public void lol2(String message);
    public static void lol3(){ System.out.println("lol3"); } // will be ignored
    public String getName();
    public String testError();
}
```

Server-side implementation:
```java

@ApiServer
public class PersonServiceImpl implements PersonService{
    // Methods
    // ...
    public void lol(){
        TransportContext.getSourceModuleId(); // client module.id available on server side
        TransportContext.getTicket(); // and security ticket too
    }
}
```

Then ```transport-maven-plugin``` generates client transport interface.
It ignores all static and default methods, all fields and makes all methods public:

```java
@ApiClient(ticketProvider = TicketProviderImpl.class)
public interface PersonServiceTransport {
    public RequestInterface<Integer> add(String name, String email, Address address);
    public RequestInterface<Person> get(Integer id);
    public RequestInterface<Void> lol();
    public RequestInterface<Void> lol2(String message);
    public RequestInterface<String> getName();
    public RequestInterface<String> testError();
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

Next, you inject this transport interface through autowiring:

```java
@Autowired
com.transport.test.PersonServiceTransport personService;

// Sync call on any implementation with 10s timeout:
Integer id = personService.add("Test name", "test@mail.com", null)
                          .withTimeout(10_000)
                          .executeSync();

// Async call on module with moduleId = main.server and timeout = 10s

personService.get(id)
             .onModule("main.server")
             .withTimeout(10_000)
             .executeAsync(UUID.randomUUID().toString(), PersonCallback.class);

// Async callback implementation example
public class PersonCallback implements Callback<Person> {

    // **key** - used as RqUID, same value that was used during invocation
    // **result** - result of method invocation
    // This method will be called if method executed without throwing exception
    // if T is Void then result will always be **null**
    @Override
    public void onSuccess(String key, Person result) {
        System.out.println("Key: " + key);
        System.out.println("Result: " + result);
    }

    // This method will be called if method thrown exception OR execution timeout occurs
    @Override
    public void onError(String key, Throwable exception) {
        System.out.println("Exception during async call");
        exception.printStackTrace();
    }
}

```

### Configuration

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

#### Required JVM options
1. **-Dzookeeper.connection**  - host:port for ZooKeeper cluster
2. **-Dservice.port**          - port for receiving TCP connections for ZeroMQ
3. **-Dmodule.id**             - unique name of server in ZooKeeper cluster
4. **-Duse.kafka**             - if true - make all sync & async calls through Kafka, otherwise it goes through ZeroMQ
5. **-Dbootstrap.servers**     - bootstrap servers of Kafka cluster

