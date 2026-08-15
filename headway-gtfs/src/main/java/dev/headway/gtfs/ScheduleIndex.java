package dev.headway.gtfs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Answers "how often is this route <em>supposed</em> to run right now?".
 *
 * <p>This is the missing half of bunching detection. A 400 m gap between two buses means nothing on
 * its own: on a route scheduled every 4 minutes it is severe bunching, and on one scheduled every
 * 45 minutes it is two buses that happen to be near each other. Only the ratio of observed to
 * scheduled is interpretable.
 *
 * <p>MARTA publishes no {@code frequencies.txt}, so the schedule has to be reconstructed: take
 * every trip on the route and direction, keep the ones whose service runs today, sort their
 * departure times, and look at the gaps.
 */
public final class ScheduleIndex implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Trips starting within this much of the query time count towards the local headway. */
    private static final int WINDOW_SECONDS = 45 * 60;

    private final ImmutableMap<String, ImmutableList<ScheduledTrip>> byGroup;
    private final ServiceCalendar calendar;

    ScheduleIndex(Map<String, List<ScheduledTrip>> byGroup, ServiceCalendar calendar) {
        ImmutableMap.Builder<String, ImmutableList<ScheduledTrip>> builder = ImmutableMap.builder();
        byGroup.forEach((group, trips) -> {
            List<ScheduledTrip> sorted = new ArrayList<>(trips);
            sorted.sort(java.util.Comparator.comparingInt(ScheduledTrip::startSecondsOfDay));
            builder.put(group, ImmutableList.copyOf(sorted));
        });
        this.byGroup = builder.build();
        this.calendar = calendar;
    }

    /**
     * What the timetable says for one route and direction at one moment.
     *
     * @param headwaySeconds typical gap between scheduled departures around this time
     * @param averageSpeedMps implied by the timetable, including stops and dwell
     * @param tripsConsidered how many scheduled trips the estimate is based on
     */
    public record Scheduled(int headwaySeconds, double averageSpeedMps, int tripsConsidered)
            implements Serializable {

        /**
         * The distance two correctly-spaced buses should have between them.
         *
         * <p>Converting the schedule into metres rather than converting observations into minutes
         * keeps the comparison in the same units the projection already produces, so no
         * measurement is transformed twice.
         */
        public double expectedSpacingMetres() {
            return headwaySeconds * averageSpeedMps;
        }
    }

    /**
     * @param headwayGroup {@code routeShortName:directionId}, as {@link TripContext#headwayGroup}
     * @param date the operating day, which selects the service calendar
     * @param secondsOfDay time of day, since headways vary enormously between peak and evening
     */
    public Optional<Scheduled> scheduledFor(String headwayGroup, LocalDate date, int secondsOfDay) {
        ImmutableList<ScheduledTrip> trips = byGroup.get(headwayGroup);
        if (trips == null || trips.isEmpty()) {
            return Optional.empty();
        }

        Set<String> activeServices = calendar.activeOn(date);

        // Only a genuinely absent calendar disables filtering. An *empty* active set means "no
        // service operates on this date" - a date outside the feed's validity, or a day this
        // route does not run - and must exclude everything.
        //
        // Treating empty as "do not filter" was the original bug here, and it is a nasty one: on
        // a Saturday it would fold every weekday trip back in, roughly halving the apparent
        // scheduled headway and reporting a correctly spaced fleet as bunched.
        boolean filterByService = !calendar.isPermissive();

        List<ScheduledTrip> nearby = new ArrayList<>();
        for (ScheduledTrip trip : trips) {
            if (filterByService && !activeServices.contains(trip.serviceId())) {
                continue;
            }
            if (Math.abs(trip.startSecondsOfDay() - secondsOfDay) <= WINDOW_SECONDS) {
                nearby.add(trip);
            }
        }

        // Two departures are the minimum needed to observe one gap.
        if (nearby.size() < 2) {
            return Optional.empty();
        }

        // Median gap, not mean. A route with a mid-morning break between peaks would otherwise
        // have that one large hole drag the "typical" headway well above what riders experience.
        List<Integer> gaps = new ArrayList<>(nearby.size() - 1);
        for (int i = 1; i < nearby.size(); i++) {
            int gap = nearby.get(i).startSecondsOfDay() - nearby.get(i - 1).startSecondsOfDay();
            if (gap > 0) {
                gaps.add(gap);
            }
        }
        if (gaps.isEmpty()) {
            return Optional.empty();
        }
        gaps.sort(Integer::compare);
        int headwaySeconds = gaps.get(gaps.size() / 2);

        List<Double> speeds = new ArrayList<>(nearby.size());
        for (ScheduledTrip trip : nearby) {
            if (trip.shapeLengthMetres() > 0) {
                speeds.add(trip.averageSpeedMps());
            }
        }
        if (speeds.isEmpty()) {
            return Optional.empty();
        }
        speeds.sort(Double::compare);
        double speed = speeds.get(speeds.size() / 2);

        return Optional.of(new Scheduled(headwaySeconds, speed, nearby.size()));
    }

    public ServiceCalendar calendar() {
        return calendar;
    }

    public int groupCount() {
        return byGroup.size();
    }

    public int tripCount() {
        return byGroup.values().stream().mapToInt(List::size).sum();
    }

    /** Every route:direction group that has a reconstructed schedule. */
    public Set<String> groups() {
        return byGroup.keySet();
    }
}
