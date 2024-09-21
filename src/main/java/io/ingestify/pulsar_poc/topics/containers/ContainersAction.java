package io.ingestify.pulsar_poc.topics.containers;

// import java.io.IOException;

// import com.hazelcast.nio.ObjectDataInput;
// import com.hazelcast.nio.ObjectDataOutput;
// import com.hazelcast.nio.serialization.StreamSerializer;

public enum ContainersAction {
    START((byte) 0), STOP((byte) 1);


    public final byte value;

    private ContainersAction(byte value) {
        this.value = value;
    }

    public static ContainersAction fromValue(byte value) {
        switch (value) {
            case 0:
                return START;
            case 1:
                return STOP;
            default:
                throw new IllegalArgumentException("Unknown value: " + value);
        }
    }
}
