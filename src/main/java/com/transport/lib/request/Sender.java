package com.transport.lib.request;

import com.transport.lib.entities.Command;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class Sender {

    // Time period in milliseconds during which we wait for answer from server
    protected long timeout = -1;
    // Target module.id
    protected String moduleId;
    // Target command
    protected Command command;

    public abstract byte[] executeSync(byte[] message);

    public abstract void executeAsync(byte[] message);
}
