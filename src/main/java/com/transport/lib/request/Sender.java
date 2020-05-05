package com.transport.lib.request;

import com.transport.lib.entities.Command;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class Sender {
    protected long timeout = -1;
    protected String moduleId;
    protected Command command;

    public abstract byte[] executeSync(byte[] message);

    public abstract void executeAsync(byte[] message);
}
