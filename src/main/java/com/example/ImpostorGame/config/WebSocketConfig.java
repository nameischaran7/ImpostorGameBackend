package com.example.ImpostorGame.config;

import com.example.ImpostorGame.handler.GameWebSocketHandler;
import com.example.ImpostorGame.service.GameService;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final GameService gameService;

    public WebSocketConfig(GameService gameService) {
        this.gameService = gameService;
    }

    @Override
    public void registerWebSocketHandlers(
            WebSocketHandlerRegistry registry) {

        registry.addHandler(
                new GameWebSocketHandler(gameService),
                "/game"
        ).setAllowedOrigins("*");
    }
}