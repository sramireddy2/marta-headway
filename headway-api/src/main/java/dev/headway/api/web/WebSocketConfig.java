package dev.headway.api.web;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Publishes the live feed at {@code ws://host:8080/ws/live}.
 *
 * <p>A raw WebSocket rather than STOMP over SockJS. STOMP adds subscriptions, acknowledgements and
 * a message broker abstraction, all of which are worth having when clients subscribe to different
 * things — and none of which apply when every client wants the same snapshot and the browser side
 * is fifteen lines of {@code new WebSocket(...)} with no library at all. SockJS's fallback
 * transports solved a problem that was real in 2013; every browser that can run step 10's map has
 * had native WebSocket support for a decade.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final LiveSocketHandler handler;

    public WebSocketConfig(LiveSocketHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/ws/live")
                // The feed is public transit data and the socket accepts no commands - it only
                // ever writes. There is nothing here for a hostile page to steal or trigger, so
                // origin restrictions would cost more in setup friction than they buy.
                .setAllowedOriginPatterns("*");
    }
}
