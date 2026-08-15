package com.example.ImpostorGame.model;

import com.example.ImpostorGame.phase.GamePhase;
import lombok.Data;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Data
public class GameRoom {

    private String roomId;

    private List<Player> players = new ArrayList<>();

    private String imposterId;

    private String normalWord;

    private String imposterWord;

    private boolean gameStarted;

    private String currentDescriberId;

    // Players who already described in current round
    private final Set<Player> described =
            new HashSet<>();

    // Player -> description
    private final Map<Player, String> desc =
            new ConcurrentHashMap<>();

    private GamePhase gamePhase;

    // voterId -> targetId
    private Map<String, String> votes =
            new ConcurrentHashMap<>();

    private boolean meetingResultProcessed;

    // IMPORTANT:
    // List is used because description order is circular.
    private final List<Player> activePlayers =
            new ArrayList<>();

    // 45-second meeting timer
    private ScheduledFuture<?> meetingTimer;

    public GameRoom(String roomId) {
        this.roomId = roomId;
    }

    public void addPlayer(Player player) {
        players.add(player);
    }
}