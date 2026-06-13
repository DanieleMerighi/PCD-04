package pcd.ttt.common;

import java.io.Serializable;

public record GameSnapshot(Board board, GameStatus status, Mark turn) implements Serializable {
}
