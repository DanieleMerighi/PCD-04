package pcd.messages;

import java.io.Serial;
import java.io.Serializable;

public record GrantMessage(String resourceId, String processId, boolean granted, String message) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "GrantMessage{" +
                "resourceId='" + resourceId + '\'' +
                ", processId='" + processId + '\'' +
                ", granted=" + granted +
                ", message='" + message + '\'' +
                '}';
    }
}
