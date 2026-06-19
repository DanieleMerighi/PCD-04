package pcd.ttt.server;

import pcd.ttt.common.Game;
import pcd.ttt.common.GameException;
import pcd.ttt.common.Mark;
import pcd.ttt.common.Move;

class PlayerProxy implements Game {

    private final GameImpl game;
    private final Mark mark;

    PlayerProxy(GameImpl game, Mark mark) {
        this.game = game;
        this.mark = mark;
    }

    @Override
    public void makeMove(Move move) throws GameException {
        game.makeMove(mark, move);
    }
}
