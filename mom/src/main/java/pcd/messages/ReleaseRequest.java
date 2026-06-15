package pcd.messages;

import java.io.Serializable;

/**
 * Messaggio di rilascio di una sezione critica.
 * Un processo invia questo messaggio al middleware per notificare il rilascio di una risorsa.
 */
public class ReleaseRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String resourceId;
    private final String processId;

    public ReleaseRequest(String resourceId, String processId) {
        this.resourceId = resourceId;
        this.processId = processId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getProcessId() {
        return processId;
    }

    @Override
    public String toString() {
        return "ReleaseRequest{" +
                "resourceId='" + resourceId + '\'' +
                ", processId='" + processId + '\'' +
                '}';
    }
}
