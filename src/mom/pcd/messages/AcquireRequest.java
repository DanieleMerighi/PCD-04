package pcd.messages;

import java.io.Serial;
import java.io.Serializable;

/**
 * Represents a request message to acquire a lock for a critical section.
 * <p>
 * A distributed process sends this message to the central middleware (lock server)
 * to request exclusive access to a specific resource.
 *
 * @param resourceId the hierarchical identifier of the requested resource
 * @param processId  the unique identifier of the process requesting the lock
 */
public record AcquireRequest(String resourceId, String processId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "AcquireRequest{" +
                "resourceId='" + resourceId + '\'' +
                ", processId='" + processId + '\'' +
                '}';
    }
}
