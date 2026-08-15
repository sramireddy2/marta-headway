package dev.headway.api.web;

import dev.headway.api.ApiProperties;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Holds the open browser connections and pushes frames to them.
 *
 * <h2>{@code WebSocketSession} is not thread-safe, and the failure is ugly</h2>
 *
 * Two threads calling {@code sendMessage} on one session interleave their writes into the same
 * output stream. What arrives is not a lost message or an exception — it is two half-frames spliced
 * together, which the client rejects as a protocol error and closes the connection on. Spring's own
 * javadoc calls this out, and {@link ConcurrentWebSocketSessionDecorator} exists for it: it
 * serialises sends behind a lock so at most one is ever in flight per session.
 *
 * <h2>The slow client problem, and why the buffer limit is the interesting part</h2>
 *
 * Serialising sends is only half the answer. A browser on a bad connection accepts bytes slowly, so
 * {@code sendMessage} blocks. With one broadcasting thread that means <em>one</em> stalled laptop
 * stops the frames going to everyone else. The decorator's buffer limit is what prevents that: once
 * a session has more than {@code socketBufferBytes} waiting, the strategy decides its fate, and
 * {@code TERMINATE} closes it.
 *
 * <p>Disconnecting a client sounds harsh, and it is the right call. This is live data: a client
 * half a megabyte behind is looking at a map that is minutes old, so the connection has already
 * failed at its job. Closing it lets the browser reconnect and resynchronise from the next full
 * snapshot, which costs it one second. Buffering indefinitely instead would trade one slow client
 * for unbounded server memory — the same bounded-queue argument as step 4, at the other end of the
 * pipeline.
 */
@Component
public final class LiveSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(LiveSocketHandler.class);

    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final ApiProperties properties;
    private final LongAdder framesSent = new LongAdder();
    private final LongAdder sendFailures = new LongAdder();

    /**
     * Supplies the frame a brand-new connection gets immediately.
     *
     * <p>Set by {@link SnapshotBroadcaster} at construction rather than injected, which keeps the
     * dependency one-way: the broadcaster knows about the handler, not the reverse. Injecting both
     * ways would be a bean cycle. Volatile because it is written on the main thread during startup
     * and read on request threads afterwards.
     */
    private volatile Supplier<String> initialFrame = () -> null;

    public LiveSocketHandler(ApiProperties properties) {
        this.properties = properties;
    }

    void onInitialFrame(Supplier<String> supplier) {
        this.initialFrame = supplier;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        WebSocketSession guarded = new ConcurrentWebSocketSessionDecorator(
                session,
                (int) properties.socketSendTimeoutMillis(),
                properties.socketBufferBytes(),
                ConcurrentWebSocketSessionDecorator.OverflowStrategy.TERMINATE);
        sessions.put(session.getId(), guarded);
        log.info("WebSocket connected: {} ({} open)", session.getId(), sessions.size());

        // Without this a new tab shows nothing until the next broadcast tick. One second is not
        // long, but "I opened it and it was blank" is the first impression either way.
        String frame = initialFrame.get();
        if (frame != null) {
            send(guarded, frame);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
        log.info("WebSocket closed: {} ({}) - {} open", session.getId(), status, sessions.size());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.debug("WebSocket transport error on {}: {}", session.getId(), exception.toString());
        sessions.remove(session.getId());
    }

    /**
     * Sends one already-serialised frame to every connected client.
     *
     * <p>The payload arrives as a string rather than an object on purpose: serialising the snapshot
     * once and sending the same bytes N times is the whole reason this method takes what it takes.
     * Passing the object and letting each send serialise it would do identical work per client.
     */
    void broadcast(String payload) {
        for (WebSocketSession session : sessions.values()) {
            send(session, payload);
        }
    }

    private void send(WebSocketSession session, String payload) {
        try {
            session.sendMessage(new TextMessage(payload));
            framesSent.increment();
        } catch (IOException | IllegalStateException e) {
            // IllegalStateException is what the decorator throws once a session has been closed
            // for exceeding its buffer. Either way the session is gone; drop it and carry on so
            // one dead client cannot interrupt the loop over the healthy ones.
            sendFailures.increment();
            sessions.remove(session.getId());
            log.debug("Dropping session {}: {}", session.getId(), e.toString());
        }
    }

    public int connectionCount() {
        return sessions.size();
    }

    public long framesSent() {
        return framesSent.sum();
    }

    public long sendFailures() {
        return sendFailures.sum();
    }
}
