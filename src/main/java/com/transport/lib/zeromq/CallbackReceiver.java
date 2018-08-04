package com.transport.lib.zeromq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.transport.lib.zookeeper.ZKUtils;
import org.zeromq.ZMQ;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;

@SuppressWarnings("unchecked")
public class CallbackReceiver implements Runnable {

    @Override
    public void run() {
        try {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket socket =  context.socket(ZMQ.REP);
            socket.bind("tcp://" + ZKUtils.getZeroMQCallbackBindAddress());
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    byte[] bytes = socket.recv();
                    Kryo kryo = new Kryo();
                    Input input = new Input(new ByteArrayInputStream(bytes));
                    final CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                    Class callbackClass = Class.forName(callbackContainer.getListener());
                    if(callbackContainer.getResult() instanceof ExceptionHolder) {
                        Method method = callbackClass.getMethod("callBackError", String.class, String.class );
                        method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), ((ExceptionHolder)callbackContainer.getResult()).getStackTrace());
                    }else {
                        Method method = callbackClass.getMethod("callBack", String.class, Class.forName(callbackContainer.getResultClass()));
                        if(Class.forName(callbackContainer.getResultClass()).equals(Void.class)){
                            method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), null);
                        }else
                            method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            socket.close();
            if(!context.isClosed()){
                context.close();
                if(!context.isTerminated())
                    context.term();
            }
        }catch (Exception e){
            System.out.println("Error during callback receiver startup:");
            e.printStackTrace(System.out);
        }
    }

}
