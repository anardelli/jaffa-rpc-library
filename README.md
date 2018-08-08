### Transport library

This library was created to manage communication betweem multiple JVM application
through interface method calls. It supports sync/async invocations through Kafka or ZeroMQ.
Synchronized method calls also support timeouts.

### How it works internally

1. During Spring context initialization, library generates stub implementations for all transport interfaces using **ByteBuddy**.
2. All method calls from transport interfaces are intercepted using **Spring AOP**.
3. Interceptor creates **Request** containing all necessary information about method call.
4. Then, user could add timeout to this call using **withTimeout(int timeout)** method.
5. In **execute methods**:
    1. Transport library serializes **Request** using **Kryo**.
    2. Checks avaiable routes in **Zookeeper** cluster.
    3. Connects to server or throws **TransportNoRouteException**.
    4. Makes method call with **ZeroMQ** or **Kafka** depending of JVM options:
        **-Duse.kafka.for.async=false/true** or **-Duse.kafka.for.sync=false/true**
    5. Waits for answer indefinitely or **timeout** milliseconds and then throws **TransportTimeoutException** in **executeSync** method.
    6. **executeAsync()** method returns immediately after sending request, then invokes methods in class used as callback listener during invocation:

        executeAsync(**key**, **class**);

        **class** - should implement Callback interface:

        ```java
        public interface Callback<T> {

            // **key** - used as RqUID, same value that was used during invocation
            // **result** - result of method invocation

            // This method will be called if method executed without throwing exception
            // if T is Void then result will always be **null**
            public void callBack(String key, T result);

            // This method will be called if method thrown exception
            public void callBackError(String key, Throwable exception);
        }
        ```

### How it works for user

You create interface with ```@Api```annotation, for example:

```java
@Api
public interface com.transport.test.PersonService {
    public static final String TEST = "TEST";
    public int add(String name,  String email, com.transport.test.Address address);
    public com.transport.test.Person get(Integer id);
    public void lol();
    public void lol2(String message);
    public static void shit(){
        System.out.println("Shit");
    }
    public String getName();
}
```

then ```transport-maven-plugin``` generates transport interface.
It ignores all static and default methods, all fields and makes all methods public:

```java
@ApiClient
public interface com.transport.test.PersonServiceTransport {
    public RequestInterface<Integer> add(String name, String email, com.transport.test.Address address);
    public RequestInterface<com.transport.test.Person> get(Integer id);
    public RequestInterface<Void> lol();
    public RequestInterface<Void> lol2(String message);
    public RequestInterface<String> getName();
}
```

next, you inject this transport interface through autowiring:

```java
@Autowired
com.transport.test.PersonServiceTransport personService;
```

```java
//Sync call with 10s timeout:
Integer id = personService.add("James Carr", "james@zapier.com", null).withTimeout(10_000).executeSync();

//Async call on module with moduleId = main.server
public class PersonCallback implements Callback<Person> {
    @Override
    public void callBack(String key, Person result) {
        System.out.println("Key: " + key);
        System.out.println("Result: " + result);
    }

    @Override
    public void callBackError(String key, Throwable exception) {
        System.out.println("Exception during async call");
        exception.printStackTrace();
    }
}

personService.get(id).onModule("main.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
```


