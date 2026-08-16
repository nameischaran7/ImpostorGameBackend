package com.example.ImpostorGame.service;

import com.example.ImpostorGame.model.GameRoom;
import com.example.ImpostorGame.model.Player;
import com.example.ImpostorGame.model.WordPair;
import com.example.ImpostorGame.phase.GamePhase;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

@Service
public class GameService {

    private final Map<String, GameRoom> rooms =
            new ConcurrentHashMap<>();

    // sessionId -> Player
    private final Map<String, Player> players =
            new ConcurrentHashMap<>();

    // sessionId -> WebSocketSession
    private final Map<String, WebSocketSession> sessions =
            new ConcurrentHashMap<>();

    // One scheduler for all rooms
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(2);


    // ---------------------------------------------------------
    // WORD PAIRS
    // ---------------------------------------------------------

    private final List<WordPair> wordPairs = List.of(
            new WordPair("Beach", "River"),
            new WordPair("Mirror", "Window"),
            new WordPair("Teacher", "Blackboard"),
            new WordPair("Dance", "Movie"),
            new WordPair("Mountain", "Hill"),
            new WordPair("Doctor", "Hospital"),
            new WordPair("Coffee", "Tea"),
            new WordPair("Train", "Bus")
    );


    // ---------------------------------------------------------
    // ROOM
    // ---------------------------------------------------------

    public GameRoom createRoom() {

        String roomId = generateRoomId();

        GameRoom room = new GameRoom(roomId);

        rooms.put(roomId, room);

        return room;
    }


    public GameRoom getRoom(String roomId) {

        return rooms.get(roomId);
    }


    // ---------------------------------------------------------
    // SESSION / PLAYER
    // ---------------------------------------------------------

    public void addSession(WebSocketSession session) {

        sessions.put(
                session.getId(),
                session
        );
    }


    public WebSocketSession getSession(
            String sessionId) {

        return sessions.get(sessionId);
    }


    public Player createPlayer(
            String sessionId,
            String name) {

        Player player = new Player();

        player.setId(sessionId);
        player.setName(name);

        players.put(
                sessionId,
                player
        );

        return player;
    }


    public Player getPlayer(String sessionId) {

        return players.get(sessionId);
    }


    // ---------------------------------------------------------
    // JOIN ROOM
    // ---------------------------------------------------------

    public boolean joinRoom(
            String roomId,
            String sessionId) {

        GameRoom room = rooms.get(roomId);

        if (room == null) {
            return false;
        }

        Player player = players.get(sessionId);

        if (player == null) {
            return false;
        }

        player.setRoomId(roomId);
        // Assign public player ID
        player.setPublicId("P" + (room.getPlayers().size() + 1));
        room.addPlayer(player);

        // Initially everyone is alive
        room.getActivePlayers().add(player);

        return true;
    }


    // ---------------------------------------------------------
    // BROADCAST
    // ---------------------------------------------------------

    public void broadcastToRoom(
            String roomId,
            String message)
            throws IOException {

        GameRoom room = rooms.get(roomId);

        if (room == null) {
            return;
        }

        // Use players instead of activePlayers.
        // Eliminated players can still see the game.
        for (Player player : room.getPlayers()) {

            WebSocketSession session =
                    sessions.get(player.getId());

            if (session != null &&
                    session.isOpen()) {

                session.sendMessage(
                        new TextMessage(message)
                );
            }
        }
    }


    public void sendToPlayer(
            String playerId,
            String message)
            throws IOException {

        WebSocketSession session =
                sessions.get(playerId);

        if (session != null &&
                session.isOpen()) {

            session.sendMessage(
                    new TextMessage(message)
            );
        }
    }


    // ---------------------------------------------------------
    // PLAYER LIST
    // ---------------------------------------------------------

    public String getPlayerList(
            String roomId) {

        GameRoom room = rooms.get(roomId);

        if (room == null) {
            return "";
        }

        StringBuilder result =
                new StringBuilder();

        for (Player player :
                room.getPlayers()) {

            if (result.length() > 0) {
                result.append(",");
            }

            result.append(player.getPublicId())
                    .append(":")
                    .append(player.getName());
        }

        return result.toString();
    }


    // ---------------------------------------------------------
    // WORD
    // ---------------------------------------------------------

    public WordPair selectWordPair() {

        int randomIndex =
                (int) (
                        Math.random() *
                                wordPairs.size()
                );

        return wordPairs.get(randomIndex);
    }


    // ---------------------------------------------------------
    // IMPOSTER
    // ---------------------------------------------------------

    public Player selectImposter(
            String roomId) {

        GameRoom room =
                rooms.get(roomId);

        if (room == null ||
                room.getPlayers().isEmpty()) {

            return null;
        }

        List<Player> roomPlayers =
                room.getPlayers();

        int randomIndex =
                (int) (
                        Math.random() *
                                roomPlayers.size()
                );

        Player imposter =
                roomPlayers.get(randomIndex);

        room.setImposterId(
                imposter.getId()
        );

        return imposter;
    }


    // ---------------------------------------------------------
    // START GAME
    // ---------------------------------------------------------

    public void startGame(
            String roomId)
            throws IOException {

        GameRoom room =
                rooms.get(roomId);

        if (room == null) {
            return;
        }

        if (room.getPlayers().size() < 3) {
            return;
        }

        // Game starts in describing phase
        room.setGamePhase(
                GamePhase.DESCRIBING
        );


        // Select imposter
        Player imposter =
                selectImposter(roomId);


        // Select word pair
        WordPair pair =
                selectWordPair();


        room.setNormalWord(
                pair.getNormalWord()
        );

        room.setImposterWord(
                pair.getImposterWord()
        );


        // Select first active describer
        Player firstDescriber =
                selectFirstActiveDescriber(
                        roomId
                );


        if (firstDescriber != null) {

            broadcastToRoom(
                    roomId,
                    "YOUR_TURN:" +
                            firstDescriber.getPublicId()
            );
        }


        room.setGameStarted(true);


        // Give every player their word
        for (Player player :
                room.getPlayers()) {

            String word;

            if (player.getId().equals(
                    room.getImposterId())) {

                word =
                        room.getImposterWord();

            } else {

                word =
                        room.getNormalWord();
            }

            sendToPlayer(
                    player.getId(),
                    "YOUR_WORD:" + word
            );
        }


        System.out.println(
                "Game started in room: " +
                        roomId
        );

        System.out.println(
                "Imposter: " +
                        imposter.getName()
        );

        System.out.println(
                "Normal word: " +
                        pair.getNormalWord()
        );

        System.out.println(
                "Imposter word: " +
                        pair.getImposterWord()
        );
    }


    // ---------------------------------------------------------
    // SELECT FIRST ACTIVE DESCRIBER
    // ---------------------------------------------------------

    public Player selectFirstActiveDescriber(
            String roomId) {

        GameRoom room =
                rooms.get(roomId);

        if (room == null ||
                room.getActivePlayers().isEmpty()) {

            return null;
        }


        List<Player> activePlayers =
                room.getActivePlayers();


        int randomIndex =
                (int) (
                        Math.random() *
                                activePlayers.size()
                );


        Player player =
                activePlayers.get(randomIndex);


        room.setCurrentDescriberId(
                player.getId()
        );

        return player;
    }


    // ---------------------------------------------------------
    // DESCRIPTION
    // ---------------------------------------------------------

    public void submitDescription(
            String playerId,
            String description)
            throws IOException {

        Player player =
                players.get(playerId);

        if (player == null) {
            return;
        }


        String roomId =
                player.getRoomId();


        GameRoom room =
                rooms.get(roomId);


        if (room == null) {
            return;
        }


        // Must be describing phase
        if (room.getGamePhase()
                != GamePhase.DESCRIBING) {

            sendToPlayer(
                    playerId,
                    "Description phase is over"
            );

            return;
        }


        // Must be alive
        if (!room.getActivePlayers()
                .contains(player)) {

            sendToPlayer(
                    playerId,
                    "You are eliminated"
            );

            return;
        }


        // Must be current describer
        if (!room.getCurrentDescriberId()
                .equals(playerId)) {

            sendToPlayer(
                    playerId,
                    "Not your turn"
            );

            return;
        }


        // Prevent duplicate description
        if (room.getDescribed()
                .contains(player)) {

            sendToPlayer(
                    playerId,
                    "You already described"
            );

            return;
        }


        // Store description
        room.getDesc().put(
                player,
                description
        );

        room.getDescribed().add(player);


        // Immediately broadcast description
        broadcastToRoom(
                roomId,
                player.getName() +
                        " : " +
                        description
        );


        // -----------------------------------------------------
        // Everyone alive has described
        // -----------------------------------------------------

        if (room.getDescribed().size()
                == room.getActivePlayers().size()) {

            meeting(roomId);

            return;
        }


        // -----------------------------------------------------
        // Find next active player
        // -----------------------------------------------------

        List<Player> activePlayers =
                room.getActivePlayers();


        int idx =
                findPlayerIndex(
                        activePlayers,
                        playerId
                );


        int nextIndex =
                (idx + 1)
                        % activePlayers.size();


        Player nextPlayer =
                activePlayers.get(nextIndex);


        room.setCurrentDescriberId(
                nextPlayer.getId()
        );


        broadcastToRoom(
                roomId,
                "YOUR_TURN:" +
                        nextPlayer.getPublicId()
        );
    }


    // ---------------------------------------------------------
    // FIND PLAYER INDEX
    // ---------------------------------------------------------

    private int findPlayerIndex(
            List<Player> players,
            String playerId) {

        for (int i = 0;
             i < players.size();
             i++) {

            if (players.get(i)
                    .getId()
                    .equals(playerId)) {

                return i;
            }
        }

        return -1;
    }


    // ---------------------------------------------------------
    // MEETING
    // ---------------------------------------------------------

    private void meeting(
            String roomId)
            throws IOException {

        GameRoom room =
                rooms.get(roomId);

        if (room == null) {
            return;
        }


        room.setGamePhase(
                GamePhase.MEETING
        );


        // Reset votes
        room.getVotes().clear();


        // Result has not been processed
        room.setMeetingResultProcessed(
                false
        );


        broadcastToRoom(
                roomId,
                "MEETING_STARTED"
        );


        broadcastToRoom(
                roomId,
                "Please Vote - 45 seconds"
        );


        // -----------------------------------------------------
        // Start 45 second timer
        // -----------------------------------------------------

        ScheduledFuture<?> future =
                scheduler.schedule(
                        () -> {

                            try {

                                // Players who didn't vote
                                // automatically SKIP
                                addMissingVotesAsSkip(
                                        roomId
                                );

                                calculateMeetingResult(
                                        roomId
                                );

                            } catch (IOException e) {

                                e.printStackTrace();
                            }

                        },
                        45,
                        TimeUnit.SECONDS
                );


        room.setMeetingTimer(
                future
        );
    }


    // ---------------------------------------------------------
    // MISSING VOTES = SKIP
    // ---------------------------------------------------------

    private void addMissingVotesAsSkip(
            String roomId) {

        GameRoom room =
                rooms.get(roomId);

        if (room == null) {
            return;
        }


        for (Player player :
                room.getActivePlayers()) {

            room.getVotes().putIfAbsent(
                    player.getId(),
                    "SKIP"
            );
        }
    }


    // ---------------------------------------------------------
    // SUBMIT VOTE
    // ---------------------------------------------------------

    public void submitVote(
            String playerId,
            String targetId)
            throws IOException {

        Player player =
                players.get(playerId);

        if (player == null) {
            return;
        }


        String roomId =
                player.getRoomId();


        GameRoom room =
                rooms.get(roomId);


        if (room == null) {
            return;
        }


        // -----------------------------------------------------
        // Must be meeting phase
        // -----------------------------------------------------

        if (room.getGamePhase()
                != GamePhase.MEETING) {

            sendToPlayer(
                    playerId,
                    "Voting is not active"
            );

            return;
        }


        // -----------------------------------------------------
        // Player must be alive
        // -----------------------------------------------------

        if (!room.getActivePlayers()
                .contains(player)) {

            sendToPlayer(
                    playerId,
                    "You are eliminated"
            );

            return;
        }


        // -----------------------------------------------------
        // One vote only
        // -----------------------------------------------------

        if (room.getVotes()
                .containsKey(playerId)) {

            sendToPlayer(
                    playerId,
                    "You already voted"
            );

            return;
        }


        // -----------------------------------------------------
        // Validate target
        // -----------------------------------------------------
        if (targetId == null || targetId.isBlank()) {
            sendToPlayer(playerId, "Invalid vote");
            return;
        }


        if (!targetId.equals("SKIP")) {

            Player target = room.getActivePlayers()
                    .stream()
                    .filter(p -> p.getPublicId().equals(targetId))
                    .findFirst()
                    .orElse(null);


            if (target == null ||
                    !room.getActivePlayers()
                            .contains(target)) {

                sendToPlayer(
                        playerId,
                        "Invalid target"
                );

                return;
            }
        }


        // -----------------------------------------------------
        // Store vote
        // -----------------------------------------------------

        room.getVotes().put(
                playerId,
                targetId
        );


        broadcastToRoom(
                roomId,
                player.getName() +
                        " voted"
        );


        // -----------------------------------------------------
        // Everyone alive voted?
        // -----------------------------------------------------

        if (room.getVotes().size()
                == room.getActivePlayers().size()) {


            // Cancel 45-second timer
            ScheduledFuture<?> timer =
                    room.getMeetingTimer();


            if (timer != null) {

                timer.cancel(false);

                room.setMeetingTimer(null);
            }


            calculateMeetingResult(
                    roomId
            );
        }
    }


    // ---------------------------------------------------------
    // CALCULATE MEETING RESULT
    // ---------------------------------------------------------

    private void calculateMeetingResult(
            String roomId)
            throws IOException {

        GameRoom room =
                rooms.get(roomId);


        if (room == null) {
            return;
        }


        // -----------------------------------------------------
        // Prevent timer + last vote from running twice
        // -----------------------------------------------------

        synchronized (room) {

            if (room.isMeetingResultProcessed()) {
                return;
            }

            room.setMeetingResultProcessed(
                    true
            );
        }


        // -----------------------------------------------------
        // Count votes
        // -----------------------------------------------------

        Map<String, Integer> voteCount =
                new HashMap<>();


        for (String targetId :
                room.getVotes().values()) {

            voteCount.put(
                    targetId,
                    voteCount.getOrDefault(
                            targetId,
                            0
                    ) + 1
            );
        }


        // -----------------------------------------------------
        // Find maximum votes
        // -----------------------------------------------------

        int maxVotes = 0;

        int numberOfPlayersWithMaxVotes = 0;

        String eliminatedId = null;


        for (Map.Entry<String, Integer> entry :
                voteCount.entrySet()) {

            int count =
                    entry.getValue();


            if (count > maxVotes) {

                maxVotes = count;

                numberOfPlayersWithMaxVotes = 1;

                eliminatedId =
                        entry.getKey();

            } else if (count == maxVotes) {

                numberOfPlayersWithMaxVotes++;
            }
        }


        // -----------------------------------------------------
        // Nobody voted / all somehow skipped
        // -----------------------------------------------------

        if (maxVotes == 0) {

            broadcastToRoom(
                    roomId,
                    "Nobody voted - Next round"
            );

            startNextRound(roomId);

            return;
        }


        // -----------------------------------------------------
        // Tie
        // -----------------------------------------------------

        if (numberOfPlayersWithMaxVotes > 1) {

            broadcastToRoom(
                    roomId,
                    "Tie - Nobody eliminated"
            );

            startNextRound(roomId);

            return;
        }


        // -----------------------------------------------------
        // SKIP has highest votes
        // -----------------------------------------------------

        if (eliminatedId.equals("SKIP")) {

            broadcastToRoom(
                    roomId,
                    "SKIP - Nobody eliminated"
            );

            startNextRound(roomId);

            return;
        }


        // -----------------------------------------------------
        // Eliminate player
        // -----------------------------------------------------

        String finalEliminatedId = eliminatedId;
        Player eliminated = room.getActivePlayers()
                .stream()
                .filter(p -> p.getPublicId().equals(finalEliminatedId))
                .findFirst()
                .orElse(null);


        if (eliminated == null) {
            return;
        }


        room.getActivePlayers()
                .remove(eliminated);


        broadcastToRoom(
                roomId,
                eliminated.getName() +
                        " was eliminated"
        );


        // -----------------------------------------------------
        // Imposter eliminated
        // -----------------------------------------------------

        if (eliminated.getId()
                .equals(room.getImposterId())) {

            room.setGamePhase(
                    GamePhase.FINISHED
            );


            broadcastToRoom(
                    roomId,
                    "NORMAL PLAYERS WIN!"
            );

            return;
        }


        // -----------------------------------------------------
        // Only two players remain
        // -----------------------------------------------------

        if (room.getActivePlayers()
                .size() == 2) {

            room.setGamePhase(
                    GamePhase.FINISHED
            );


            broadcastToRoom(
                    roomId,
                    "IMPOSTER WINS!"
            );

            return;
        }


        // -----------------------------------------------------
        // Continue to next round
        // -----------------------------------------------------

        startNextRound(roomId);
    }


    // ---------------------------------------------------------
    // NEXT ROUND
    // ---------------------------------------------------------

    private void startNextRound(
            String roomId)
            throws IOException {

        GameRoom room =
                rooms.get(roomId);


        if (room == null) {
            return;
        }


        // -----------------------------------------------------
        // Clear previous round
        // -----------------------------------------------------

        room.getDescribed().clear();

        room.getDesc().clear();

        room.getVotes().clear();

        room.setMeetingResultProcessed(
                false
        );


        // -----------------------------------------------------
        // Select random active player
        // -----------------------------------------------------

        Player first =
                selectFirstActiveDescriber(
                        roomId
                );


        if (first == null) {
            return;
        }


        room.setGamePhase(
                GamePhase.DESCRIBING
        );


        broadcastToRoom(
                roomId,
                "NEXT_ROUND"
        );


        broadcastToRoom(
                roomId,
                "Your Turn : " +
                        first.getName()
        );
    }


    // ---------------------------------------------------------
    // ROOM ID
    // ---------------------------------------------------------

    private String generateRoomId() {

        return String.valueOf(
                (int) (
                        Math.random() * 9000
                ) + 1000
        );
    }
}