package pcd.messages;

import java.io.Serial;
import java.io.Serializable;

/**
 * Represents a response message from the lock server indicating the outcome of an acquire request.
 * <p>
 * The central middleware sends this message back to the requesting process to notify
 * whether the lock on the specified resource has been granted.
 *
 * @param resourceId the hierarchical identifier of the requested resource
 * @param processId  the unique identifier of the target process receiving this message
 * @param granted    {@code true} if the lock has been successfully acquired, {@code false} otherwise
 * @param message    an optional descriptive message from the server (e.g., "Lock Granted")
 */
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
