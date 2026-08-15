package com.example.ImpostorGame.handler;

import com.example.ImpostorGame.model.GameRoom;
import com.example.ImpostorGame.model.Player;
import com.example.ImpostorGame.model.ClientMessage;
import com.example.ImpostorGame.model.WordPair;
import com.example.ImpostorGame.service.GameService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.socket.*;

import java.io.IOException;

public class GameWebSocketHandler implements WebSocketHandler {

    private final GameService gameService;

    // Reuse one ObjectMapper
    private final ObjectMapper objectMapper =
            new ObjectMapper();

    public GameWebSocketHandler(GameService gameService) {
        this.gameService = gameService;
    }

    @Override
    public void afterConnectionEstablished(
            WebSocketSession session) {

        gameService.addSession(session);

        Player player = gameService.createPlayer(
                session.getId(),
                "Player-" +
                        session.getId().substring(0, 4)
        );

        System.out.println(
                "Player connected: " +
                        player.getName()
        );
    }

    @Override
    public void handleMessage(
            WebSocketSession session,
            WebSocketMessage<?> message)
            throws IOException {

        String messageText =
                message.getPayload().toString();

        System.out.println(
                "Received: " + messageText
        );

        // =====================================================
        // JSON MESSAGES
        // =====================================================

        if (messageText.startsWith("{")) {

            ClientMessage request =
                    objectMapper.readValue(
                            messageText,
                            ClientMessage.class
                    );

            // -----------------------------
            // DESCRIPTION
            // -----------------------------

            if ("DESCRIPTION".equals(
                    request.getType())) {

                gameService.submitDescription(
                        session.getId(),
                        request.getDescription()
                );
            }

            // -----------------------------
            // VOTE
            // -----------------------------

            else if ("VOTE".equals(
                    request.getType())) {

                gameService.submitVote(
                        session.getId(),
                        request.getTargetId()
                );
            }

            // -----------------------------
            // UNKNOWN JSON TYPE
            // -----------------------------

            else {

                session.sendMessage(
                        new TextMessage(
                                "Unknown message type"
                        )
                );
            }

            return;
        }


        // =====================================================
        // OLD STRING COMMANDS
        // =====================================================

        // CREATE ROOM
        if (messageText.equals("CREATE_ROOM")) {

            GameRoom room =
                    gameService.createRoom();

            session.sendMessage(
                    new TextMessage(
                            "ROOM_CREATED:" +
                                    room.getRoomId()
                    )
            );
        }


        // JOIN ROOM
        if (messageText.startsWith(
                "JOIN_ROOM:")) {

            String roomId =
                    messageText.substring(10);

            boolean joined =
                    gameService.joinRoom(
                            roomId,
                            session.getId()
                    );

            if (joined) {

                String playerList =
                        gameService.getPlayerList(
                                roomId
                        );

                gameService.broadcastToRoom(
                        roomId,
                        "PLAYERS:" +
                                playerList
                );

            } else {

                session.sendMessage(
                        new TextMessage(
                                "ROOM_NOT_FOUND"
                        )
                );
            }
        }


        // START GAME
        if (messageText.equals("START_GAME")) {

            Player player =
                    gameService.getPlayer(
                            session.getId()
                    );

            if (player == null ||
                    player.getRoomId() == null) {

                session.sendMessage(
                        new TextMessage(
                                "NOT_IN_ROOM"
                        )
                );

                return;
            }

            gameService.startGame(
                    player.getRoomId()
            );
        }


        // SELECT IMPOSTER
        if (messageText.equals(
                "SELECT_IMPOSTER")) {

            Player player =
                    gameService.getPlayer(
                            session.getId()
                    );

            if (player == null ||
                    player.getRoomId() == null) {

                session.sendMessage(
                        new TextMessage(
                                "NOT_IN_ROOM"
                        )
                );

                return;
            }

            String roomId =
                    player.getRoomId();

            Player imposter =
                    gameService.selectImposter(
                            roomId
                    );

            if (imposter != null) {

                System.out.println(
                        "Imposter selected: " +
                                imposter.getName()
                );
            }
        }


        // SELECT WORD
        if (messageText.equals(
                "SELECT_WORD")) {

            WordPair pair =
                    gameService.selectWordPair();

            System.out.println(
                    "Normal word: " +
                            pair.getNormalWord()
            );

            System.out.println(
                    "Imposter word: " +
                            pair.getImposterWord()
            );
        }
    }

    @Override
    public void handleTransportError(
            WebSocketSession session,
            Throwable exception) {

        System.out.println(
                "WebSocket error: " +
                        exception.getMessage()
        );
    }

    @Override
    public void afterConnectionClosed(
            WebSocketSession session,
            CloseStatus status) {

        System.out.println(
                "Player disconnected: " +
                        session.getId()
        );
    }

    @Override
    public boolean supportsPartialMessages() {
        return false;
    }
}