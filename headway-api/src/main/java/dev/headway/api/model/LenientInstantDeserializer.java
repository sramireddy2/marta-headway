package dev.headway.api.model;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

/**
 * Reads a timestamp that Spark wrote, in whichever of its two shapes it used.
 *
 * <p>Jackson's own {@code InstantDeserializer} requires a zone or offset: {@code
 * 2026-08-15T14:32:00Z} parses, {@code 2026-08-15T14:32:00.000} does not. Spark writes the second
 * form whenever its JSON {@code timestampFormat} resolves without an offset, which depends on the
 * session time zone and the Spark version — configuration that lives in the stream job, not here.
 *
 * <p>Being strict about this would mean a working pipeline where every record fails to parse in the
 * API and the dashboard is simply empty, with the cause buried in a log line. Being lenient costs
 * one class. The stream job sets {@code spark.sql.session.timeZone=UTC}, so a value with no offset
 * is UTC — that assumption is the one thing here worth remembering, because it is silently wrong if
 * anyone changes that setting.
 */
public final class LenientInstantDeserializer extends JsonDeserializer<Instant> {

    @Override
    public Instant deserialize(JsonParser parser, DeserializationContext context)
            throws IOException, JacksonException {

        // Spark can also emit epoch numbers, depending on how the JSON was produced.
        if (parser.currentToken() != null && parser.currentToken().isNumeric()) {
            return Instant.ofEpochMilli(parser.getLongValue());
        }

        String text = parser.getValueAsString();
        if (text == null || text.isBlank()) {
            return null;
        }
        String trimmed = text.trim();
        try {
            return Instant.parse(trimmed);
        } catch (DateTimeParseException withoutOffset) {
            try {
                return LocalDateTime.parse(trimmed).toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException stillBad) {
                // Report the original failure: "no offset" is the more useful diagnosis than
                // "not a local date-time either".
                throw new IOException("Unparseable timestamp: " + trimmed, withoutOffset);
            }
        }
    }
}
