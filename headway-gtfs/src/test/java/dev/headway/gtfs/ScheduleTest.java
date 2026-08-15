package dev.headway.gtfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.google.common.collect.ImmutableSet;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Service calendars, GTFS clock arithmetic, and reconstructing a headway from a timetable. */
class ScheduleTest {

    @Nested
    @DisplayName("GTFS times, which are not wall-clock times")
    class GtfsTimes {

        @Test
        @DisplayName("an ordinary time parses to seconds since midnight")
        void ordinaryTime() {
            assertThat(ScheduledTrip.parseGtfsTime("06:20:00")).isEqualTo(6 * 3600 + 20 * 60);
            assertThat(ScheduledTrip.parseGtfsTime("00:00:00")).isZero();
            assertThat(ScheduledTrip.parseGtfsTime("23:59:59")).isEqualTo(86_399);
        }

        /**
         * MARTA's feed contains 88,862 rows with an hour of 24 or more, up to 26. These are
         * after-midnight services belonging to the previous operating day.
         * {@code LocalTime.parse("25:30:00")} throws, which is why this is hand-rolled.
         */
        @Test
        @DisplayName("hours past 24 are valid and must not throw")
        void afterMidnight() {
            assertThat(ScheduledTrip.parseGtfsTime("24:00:00")).isEqualTo(86_400);
            assertThat(ScheduledTrip.parseGtfsTime("25:30:00")).isEqualTo(25 * 3600 + 30 * 60);
            assertThat(ScheduledTrip.parseGtfsTime("26:15:30")).isEqualTo(26 * 3600 + 15 * 60 + 30);
        }

        /** MARTA writes {@code " 6:20:00"} with a leading space and no zero padding. */
        @Test
        @DisplayName("leading spaces and single-digit hours are tolerated")
        void martaFormatting() {
            assertThat(ScheduledTrip.parseGtfsTime(" 6:20:00")).isEqualTo(6 * 3600 + 20 * 60);
            assertThat(ScheduledTrip.parseGtfsTime("  6:20:00 ")).isEqualTo(6 * 3600 + 20 * 60);
        }

        @Test
        @DisplayName("unparseable values return -1 rather than throwing")
        void badInput() {
            assertThat(ScheduledTrip.parseGtfsTime("")).isEqualTo(-1);
            assertThat(ScheduledTrip.parseGtfsTime(null)).isEqualTo(-1);
            assertThat(ScheduledTrip.parseGtfsTime("not a time")).isEqualTo(-1);
            assertThat(ScheduledTrip.parseGtfsTime("12:99:00")).isEqualTo(-1);
        }

        @Test
        @DisplayName("average speed comes from length over scheduled duration")
        void averageSpeed() {
            // 10 km in 40 minutes = 4.166 m/s, which includes every scheduled stop.
            ScheduledTrip trip = new ScheduledTrip("t", "5", 6 * 3600, 6 * 3600 + 2400, 10_000);

            assertThat(trip.durationSeconds()).isEqualTo(2400);
            assertThat(trip.averageSpeedMps()).isCloseTo(4.167, within(0.01));
        }
    }

    @Nested
    @DisplayName("service calendars")
    class Calendars {

        private ServiceCalendar martaLike() {
            LocalDate from = LocalDate.of(2026, 6, 27);
            LocalDate to = LocalDate.of(2026, 8, 21);
            return new ServiceCalendar(
                    Map.of(
                            "5", new ServiceCalendar.WeeklyService("5",
                                    Set.of(DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY,
                                            DayOfWeek.THURSDAY, DayOfWeek.FRIDAY), from, to),
                            "3", new ServiceCalendar.WeeklyService("3",
                                    Set.of(DayOfWeek.SATURDAY), from, to),
                            "4", new ServiceCalendar.WeeklyService("4",
                                    Set.of(DayOfWeek.SUNDAY), from, to),
                            "29", new ServiceCalendar.WeeklyService("29",
                                    Set.of(), from, to)),          // holiday-only, all days zero
                    Map.of(LocalDate.of(2026, 7, 4), ImmutableSet.of("29")),
                    Map.of(LocalDate.of(2026, 7, 4), ImmutableSet.of("3")));
        }

        @Test
        @DisplayName("weekday, Saturday and Sunday select different services")
        void weeklyPattern() {
            ServiceCalendar calendar = martaLike();

            assertThat(calendar.activeOn(LocalDate.of(2026, 8, 14))).containsExactly("5"); // Friday
            assertThat(calendar.activeOn(LocalDate.of(2026, 8, 15))).containsExactly("3"); // Saturday
            assertThat(calendar.activeOn(LocalDate.of(2026, 8, 16))).containsExactly("4"); // Sunday
        }

        /**
         * The exception file overrides the weekly pattern. 2026-07-04 is a Saturday in this feed;
         * service 3 is removed and holiday service 29 is added. Applying these in the wrong order
         * would run a normal Saturday schedule on Independence Day.
         */
        @Test
        @DisplayName("calendar_dates exceptions override the weekly pattern")
        void holidayExceptions() {
            ServiceCalendar calendar = martaLike();

            Set<String> july4 = calendar.activeOn(LocalDate.of(2026, 7, 4));

            assertThat(july4).contains("29");
            assertThat(july4).doesNotContain("3");
        }

        @Test
        @DisplayName("dates outside the feed's validity range select nothing")
        void outsideValidity() {
            ServiceCalendar calendar = martaLike();

            assertThat(calendar.activeOn(LocalDate.of(2026, 6, 1))).isEmpty();
            assertThat(calendar.covers(LocalDate.of(2026, 6, 1))).isFalse();
            assertThat(calendar.covers(LocalDate.of(2026, 8, 14))).isTrue();
        }

        @Test
        @DisplayName("a feed with no calendar files does not filter anything out")
        void permissive() {
            assertThat(ServiceCalendar.permissive().isPermissive()).isTrue();
            assertThat(ServiceCalendar.permissive().covers(LocalDate.of(2030, 1, 1))).isTrue();
        }
    }

    @Nested
    @DisplayName("reconstructing scheduled headway")
    class ScheduledHeadways {

        /** Route 15 outbound: every 10 minutes from 08:00, 12 km taking 40 minutes. */
        private ScheduleIndex everyTenMinutes(ServiceCalendar calendar, String serviceId) {
            List<ScheduledTrip> trips = new java.util.ArrayList<>();
            for (int i = 0; i < 12; i++) {
                int start = 8 * 3600 + i * 600;
                trips.add(new ScheduledTrip("t" + i, serviceId, start, start + 2400, 12_000));
            }
            return new ScheduleIndex(Map.of("15:0", trips), calendar);
        }

        private ServiceCalendar weekdayOnly() {
            return new ServiceCalendar(
                    Map.of("5", new ServiceCalendar.WeeklyService("5",
                            Set.of(DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY,
                                    DayOfWeek.THURSDAY, DayOfWeek.FRIDAY),
                            LocalDate.of(2026, 1, 1), LocalDate.of(2026, 12, 31))),
                    Map.of(), Map.of());
        }

        @Test
        @DisplayName("a 10-minute timetable is recovered as a 600-second headway")
        void recoversHeadway() {
            ScheduleIndex index = everyTenMinutes(weekdayOnly(), "5");

            ScheduleIndex.Scheduled scheduled = index
                    .scheduledFor("15:0", LocalDate.of(2026, 8, 14), 9 * 3600)
                    .orElseThrow();

            assertThat(scheduled.headwaySeconds()).isEqualTo(600);
            // 12 km in 40 minutes = 5 m/s.
            assertThat(scheduled.averageSpeedMps()).isCloseTo(5.0, within(0.01));
            // Correctly spaced buses are 600 s x 5 m/s = 3000 m apart.
            assertThat(scheduled.expectedSpacingMetres()).isCloseTo(3000.0, within(1.0));
        }

        /**
         * Without the calendar filter, weekday and weekend trips merge and the apparent headway
         * halves — which would make a perfectly spaced fleet look bunched.
         */
        @Test
        @DisplayName("trips whose service does not run today are excluded")
        void filtersByServiceDay() {
            ScheduleIndex weekdayIndex = everyTenMinutes(weekdayOnly(), "5");

            assertThat(weekdayIndex.scheduledFor("15:0", LocalDate.of(2026, 8, 14), 9 * 3600))
                    .as("Friday: weekday service runs").isPresent();
            assertThat(weekdayIndex.scheduledFor("15:0", LocalDate.of(2026, 8, 15), 9 * 3600))
                    .as("Saturday: no weekday trips, so nothing to compare against").isEmpty();
        }

        @Test
        @DisplayName("a time with no nearby departures yields nothing rather than a guess")
        void quietHoursYieldNothing() {
            ScheduleIndex index = everyTenMinutes(weekdayOnly(), "5");

            assertThat(index.scheduledFor("15:0", LocalDate.of(2026, 8, 14), 3 * 3600)).isEmpty();
        }

        @Test
        @DisplayName("an unknown route yields nothing")
        void unknownGroup() {
            ScheduleIndex index = everyTenMinutes(weekdayOnly(), "5");

            assertThat(index.scheduledFor("999:0", LocalDate.of(2026, 8, 14), 9 * 3600)).isEmpty();
        }

        /**
         * Headways vary through the day. Looking up the wrong time of day compares a rush-hour
         * service against an evening one and inverts the verdict.
         */
        @Test
        @DisplayName("peak and off-peak give different headways")
        void headwayVariesByTimeOfDay() {
            List<ScheduledTrip> trips = new java.util.ArrayList<>();
            for (int i = 0; i < 12; i++) {           // 07:00-09:00, every 10 min
                int start = 7 * 3600 + i * 600;
                trips.add(new ScheduledTrip("peak" + i, "5", start, start + 2400, 12_000));
            }
            for (int i = 0; i < 6; i++) {            // 20:00-23:00, every 36 min
                int start = 20 * 3600 + i * 2160;
                trips.add(new ScheduledTrip("eve" + i, "5", start, start + 2400, 12_000));
            }
            ScheduleIndex index = new ScheduleIndex(Map.of("15:0", trips), weekdayOnly());
            LocalDate friday = LocalDate.of(2026, 8, 14);

            assertThat(index.scheduledFor("15:0", friday, 8 * 3600).orElseThrow()
                    .headwaySeconds()).isEqualTo(600);
            assertThat(index.scheduledFor("15:0", friday, 21 * 3600 + 1800).orElseThrow()
                    .headwaySeconds()).isEqualTo(2160);
        }

        /** A mid-morning break between peaks must not drag the typical headway up. */
        @Test
        @DisplayName("the median resists one large hole in the timetable")
        void medianResistsATimetableHole() {
            List<ScheduledTrip> trips = List.of(
                    new ScheduledTrip("a", "5", 9 * 3600, 9 * 3600 + 2400, 12_000),
                    new ScheduledTrip("b", "5", 9 * 3600 + 600, 9 * 3600 + 3000, 12_000),
                    new ScheduledTrip("c", "5", 9 * 3600 + 1200, 9 * 3600 + 3600, 12_000),
                    new ScheduledTrip("d", "5", 9 * 3600 + 3600, 9 * 3600 + 6000, 12_000));
            ScheduleIndex index = new ScheduleIndex(Map.of("15:0", trips), weekdayOnly());

            // Gaps are 600, 600, 2400. Median 600; the mean would be 1200.
            assertThat(index.scheduledFor("15:0", LocalDate.of(2026, 8, 14), 9 * 3600 + 600)
                    .orElseThrow().headwaySeconds()).isEqualTo(600);
        }
    }
}
