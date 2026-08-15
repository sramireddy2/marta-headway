package dev.headway.gtfs;

import java.io.Serializable;

/**
 * One scheduled run, reduced to the three numbers the headway comparison needs.
 *
 * <h2>Why seconds since midnight and not {@code LocalTime}</h2>
 *
 * GTFS times are allowed to exceed 24:00:00, and MARTA uses that heavily: 88,862 rows in
 * {@code stop_times.txt} have an hour of 24 or more, with a maximum of 26. A trip that departs at
 * 25:30:00 is a 1:30 AM service belonging to the <em>previous</em> operating day — which matters,
 * because it runs on the previous day's service calendar.
 *
 * <p>{@link java.time.LocalTime#parse} throws on "25:30:00". Storing plain seconds since the start
 * of the service day keeps the arithmetic trivial and preserves the distinction between "late
 * Friday night" and "early Saturday morning" that a wall clock destroys.
 *
 * @param tripId the scheduled run
 * @param serviceId which calendar decides whether it operates on a given date
 * @param startSecondsOfDay departure from the first stop
 * @param endSecondsOfDay arrival at the last stop
 * @param shapeLengthMetres how far the trip travels, used to derive an average speed
 */
public record ScheduledTrip(
        String tripId,
        String serviceId,
        int startSecondsOfDay,
        int endSecondsOfDay,
        double shapeLengthMetres) implements Serializable {

    /** Scheduled running time. Never zero for a real trip, so it is safe to divide by. */
    public int durationSeconds() {
        return Math.max(1, endSecondsOfDay - startSecondsOfDay);
    }

    /**
     * The speed the timetable implies, in metres per second.
     *
     * <p>Not a free-flow speed: it already includes every scheduled stop, dwell and allowance,
     * which is exactly what is wanted when converting a distance between buses into the time
     * riders will actually wait.
     */
    public double averageSpeedMps() {
        return shapeLengthMetres / durationSeconds();
    }

    /**
     * Parses a GTFS {@code HH:MM:SS}, tolerating hours past 24 and the leading spaces MARTA emits
     * (its rows literally read {@code " 6:20:00"}).
     *
     * @return seconds since the start of the service day, or -1 if unparseable
     */
    public static int parseGtfsTime(String raw) {
        if (raw == null) {
            return -1;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            return -1;
        }
        int first = value.indexOf(':');
        int second = value.indexOf(':', first + 1);
        if (first < 0 || second < 0) {
            return -1;
        }
        try {
            int hours = Integer.parseInt(value.substring(0, first));
            int minutes = Integer.parseInt(value.substring(first + 1, second));
            int seconds = Integer.parseInt(value.substring(second + 1));
            if (hours < 0 || minutes < 0 || minutes > 59 || seconds < 0 || seconds > 59) {
                return -1;
            }
            return hours * 3600 + minutes * 60 + seconds;
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
