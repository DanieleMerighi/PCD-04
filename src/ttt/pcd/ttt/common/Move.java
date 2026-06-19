package pcd.ttt.common;

import java.io.Serializable;

public record Move(int row, int col) implements Serializable {

    public Move {
        if (row < 0 || col < 0) {
            throw new IllegalArgumentException("Invalid coordinates: (" + row + ", " + col + ")");
        }
    }
}
