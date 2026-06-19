package pcd.messages;

import java.io.Serial;
import java.io.Serializable;

/**
 * Represents a request message to release a previously acquired lock.
 * <p>
 * A distributed process sends this message to the central middleware (lock server)
 * to notify that it has finished its critical section and the resource can be freed.
 *
 * @param resourceId the hierarchical identifier of the resource to be released
 * @param processId  the unique identifier of the process releasing the lock
 */
public record ReleaseRequest(String resourceId, String processId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "ReleaseRequest{" +
                "resourceId='" + resourceId + '\'' +
                ", processId='" + processId + '\'' +
                '}';
    }
}
