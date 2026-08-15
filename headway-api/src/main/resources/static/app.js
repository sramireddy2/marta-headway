/*
 * Headway live map.
 *
 * Consumes the snapshot frames from /ws/live and draws them. Roughly 180 buses, one frame a
 * second, on a machine that is also running Kafka, Spark and a JVM - so the two things that matter
 * here are not features, they are:
 *
 *   1. Reuse DOM and Leaflet objects instead of recreating them every frame.
 *   2. Reconnect politely when the socket drops.
 *
 * Everything else is formatting.
 */
(() => {
    'use strict';

    /*
     * Status colours are read from the stylesheet rather than repeated here.
     *
     * The legend swatches and the map dots have to agree, and two hard-coded lists of hex values
     * agree only until the day someone edits one of them. There is exactly one definition, in
     * style.css, and this pulls from it.
     */
    const css = getComputedStyle(document.documentElement);
    const STATUSES = ['SEVERE_BUNCHING', 'BUNCHING', 'ON_SCHEDULE', 'GAPPING', 'SEVERE_GAPPING', 'LAYOVER'];
    const COLOUR = {};
    for (const status of STATUSES) {
        COLOUR[status] = css.getPropertyValue('--' + cssClass(status)).trim();
    }
    const UNKNOWN = COLOUR.LAYOVER;

    function cssClass(status) {
        return status ? status.toLowerCase().replace(/_/g, '-') : 'layover';
    }

    // ---------------------------------------------------------------- map

    /*
     * preferCanvas draws every marker into one <canvas> instead of giving each its own SVG node.
     * At 180 buses redrawn once a second the difference is the page staying responsive versus the
     * browser spending its life in layout. The cost is that markers cannot be styled with CSS -
     * which is why the colours above are read out of the stylesheet by hand.
     */
    const map = L.map('map', { preferCanvas: true, zoomControl: true })
        .setView([33.755, -84.39], 11);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        maxZoom: 19,
        subdomains: 'abcd',
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> '
            + 'contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
    }).addTo(map);

    const markers = new Map();      // vehicleId -> L.CircleMarker
    let fittedOnce = false;

    /*
     * Re-measure the map whenever it might have changed size. This is the fix for the single most
     * common Leaflet complaint - "the map is blank until I resize the window".
     *
     * Leaflet measures its container once at construction and then only on a window resize. Two
     * ordinary situations break that. Open this page in a background tab and the browser suspends
     * animation frames, so Leaflet's deferred sizing work never runs; switching to the tab fires no
     * resize event, so nothing ever corrects it and the map sits there empty. Collapsing the alert
     * panel changes the map's width without changing the window's at all.
     *
     * Verified rather than assumed: loaded hidden, this page reports document.hidden true, zero
     * animation frames in half a second, a correctly sized 1100x844 container - and a 0x0 canvas.
     *
     * A ResizeObserver watches the element itself rather than the window, which covers both cases
     * and every future one. invalidateSize() does not change the container's size, so observing the
     * thing we then re-measure cannot loop.
     */
    const mapEl = document.getElementById('map');
    new ResizeObserver(() => map.invalidateSize()).observe(mapEl);
    document.addEventListener('visibilitychange', () => {
        if (!document.hidden) map.invalidateSize();
    });

    // ---------------------------------------------------------------- rendering

    /**
     * Which status applies to each bus.
     *
     * A vehicle position carries a routeId but no usable direction - MARTA's directionId is not the
     * GTFS 0/1 flag, as headway-common documents at length. So the direction, and therefore the
     * headway group, cannot be derived from the bus. It has to come the other way round: each route
     * record already lists the vehicles it measured, in order, so inverting that mapping gives every
     * bus its group's verdict without guessing anything.
     */
    function statusIndex(routes) {
        const byVehicle = new Map();
        for (const route of routes) {
            for (const id of route.layoverVehicles || []) {
                byVehicle.set(id, { status: 'LAYOVER', route });
            }
            if (!route.status) continue;
            for (const id of route.orderedVehicles || []) {
                byVehicle.set(id, { status: route.status, route });
            }
        }
        return byVehicle;
    }

    function render(snapshot) {
        const byVehicle = statusIndex(snapshot.routes || []);

        // Buses named in an open episode get a ring: these are the specific pairs a dispatcher
        // would act on, as opposed to every bus on a troubled route.
        const flagged = new Set();
        for (const alert of snapshot.alerts || []) {
            for (const id of alert.vehicles || []) flagged.add(id);
        }

        const seen = new Set();
        const points = [];

        for (const bus of snapshot.vehicles || []) {
            seen.add(bus.vehicleId);
            const info = byVehicle.get(bus.vehicleId);
            const status = info ? info.status : null;
            const colour = status ? COLOUR[status] || UNKNOWN : UNKNOWN;
            const ringed = flagged.has(bus.vehicleId);
            const position = [bus.latitude, bus.longitude];
            points.push(position);

            let marker = markers.get(bus.vehicleId);
            if (!marker) {
                marker = L.circleMarker(position, { radius: 5, weight: 1.5 });
                /*
                 * The popup content is a function, not a string. Bound as a string it would be
                 * built for all 180 buses on every frame and be stale the moment it was built;
                 * as a function it runs once, when someone actually opens it, against whatever
                 * data the marker is holding at that moment.
                 */
                marker.bindPopup(() => popupHtml(marker.$bus, marker.$info));
                marker.addTo(map);
                markers.set(bus.vehicleId, marker);
            } else {
                /*
                 * setLatLng, not remove-and-recreate. Recreating every marker each second throws
                 * away and rebuilds 180 objects a second, makes the map flicker, and slams shut
                 * any popup the user is in the middle of reading.
                 */
                marker.setLatLng(position);
            }

            marker.$bus = bus;
            marker.$info = info;
            marker.setStyle({
                color: ringed ? '#ffffff' : colour,
                weight: ringed ? 2 : 1.5,
                fillColor: colour,
                fillOpacity: status ? 0.9 : 0.45,
                radius: ringed ? 7 : 5
            });
        }

        // Buses that dropped out of the feed - the API evicts them after five minutes.
        for (const [id, marker] of markers) {
            if (!seen.has(id)) {
                map.removeLayer(marker);
                markers.delete(id);
            }
        }

        if (!fittedOnce && points.length > 2) {
            map.fitBounds(L.latLngBounds(points).pad(0.05));
            fittedOnce = true;
        }

        renderAlerts(snapshot.alerts || []);
        renderStats(snapshot);
    }

    function popupHtml(bus, info) {
        const route = info && info.route;
        const status = info ? info.status : null;
        const rows = [];

        rows.push('<b>Bus ' + esc(bus.vehicleId) + '</b><br>');
        rows.push(row('Route', route
            ? esc(route.routeShortName + ' — ' + (route.routeLongName || ''))
            : esc(bus.routeId)));
        if (status) {
            rows.push(row('Status',
                '<span style="color:' + (COLOUR[status] || UNKNOWN) + '">' + label(status) + '</span>'));
        }
        if (route && route.headwayRatio != null) {
            rows.push(row('Spacing', route.headwayRatio.toFixed(2) + '&times; scheduled'));
        }
        if (route && route.minGapMetres != null) {
            rows.push(row('Closest gap', distance(route.minGapMetres)
                + (route.expectedSpacingMetres != null
                    ? ' of ' + distance(route.expectedSpacingMetres) : '')));
        }
        if (bus.speedMetersPerSecond != null) {
            // MARTA quantises speed to 5 mph buckets - see the projection notes in the README -
            // so mph is the honest unit to show it in.
            rows.push(row('Speed', Math.round(bus.speedMetersPerSecond * 2.23694) + ' mph'));
        }
        rows.push(row('Seen', ago(bus.timestamp) + ' ago'));
        return rows.join('');
    }

    const row = (key, value) => '<span class="k">' + key + '</span>' + value + '<br>';

    // ---------------------------------------------------------------- alert panel

    const alertList = document.getElementById('alerts');
    const alertsEmpty = document.getElementById('alerts-empty');
    let lastSignature = '';

    /*
     * Rebuilt only when something actually changed.
     *
     * Blowing away the list every second would reset scroll position and cancel any hover the user
     * is in the middle of - and most seconds nothing changed, because an episode absorbing another
     * window is precisely the case that should NOT disturb the display. The signature covers the
     * fields the row renders; if none of them moved, neither does the DOM.
     */
    function renderAlerts(alerts) {
        const signature = alerts
            .map(a => [a.id, a.status, a.worstStatus, a.durationSeconds, a.windowsObserved].join(','))
            .join('|');
        if (signature === lastSignature) return;
        lastSignature = signature;

        alertsEmpty.style.display = alerts.length ? 'none' : 'block';
        alertList.textContent = '';

        for (const alert of alerts) {
            const li = document.createElement('li');
            li.className = cssClass(alert.worstStatus);

            const recovering = alert.status !== alert.worstStatus;
            const ratio = alert.ratio != null ? alert.ratio.toFixed(2) + '×' : '—';

            const top = document.createElement('div');
            top.className = 'a-top';
            top.innerHTML =
                '<span class="a-route">' + esc(alert.routeShortName || alert.headwayGroup) + '</span>'
                + '<span class="a-name">' + esc(alert.routeLongName || '') + '</span>'
                + '<span class="a-ratio" style="color:' + (COLOUR[alert.worstStatus] || UNKNOWN) + '">'
                + ratio + '</span>';

            const meta = document.createElement('div');
            meta.className = 'a-meta';
            meta.innerHTML =
                label(alert.worstStatus)
                + (recovering ? ' <span class="a-recovering">&rarr; ' + label(alert.status) + '</span>' : '')
                + '<span class="sep">|</span>' + duration(alert.durationSeconds)
                + '<span class="sep">|</span>' + alert.windowsObserved + ' windows'
                + (alert.vehicles && alert.vehicles.length
                    ? '<span class="sep">|</span>buses ' + esc(alert.vehicles.join(', ')) : '');

            li.append(top, meta);
            li.addEventListener('click', () => locate(alert));
            alertList.append(li);
        }
    }

    /** Frames the two buses involved, so "route 51 is bunched" becomes "and here they are". */
    function locate(alert) {
        const points = (alert.vehicles || [])
            .map(id => markers.get(id))
            .filter(Boolean)
            .map(marker => marker.getLatLng());
        if (!points.length) return;
        if (points.length === 1) {
            map.setView(points[0], 15);
        } else {
            map.fitBounds(L.latLngBounds(points).pad(0.6), { maxZoom: 15 });
        }
    }

    // ---------------------------------------------------------------- header

    const stats = {
        buses: document.getElementById('stat-buses'),
        routes: document.getElementById('stat-routes'),
        alerts: document.getElementById('stat-alerts')
    };

    function renderStats(snapshot) {
        set(stats.buses, (snapshot.vehicles || []).length);
        set(stats.routes, (snapshot.routes || []).length);
        set(stats.alerts, (snapshot.alerts || []).length);
    }

    function set(node, value) {
        const text = String(value);
        if (node.textContent !== text) node.textContent = text;   // avoid pointless reflow
    }

    // ---------------------------------------------------------------- socket

    const connBadge = document.getElementById('conn');
    const connText = document.getElementById('conn-text');
    let socket = null;
    let attempt = 0;
    let lastFrameAt = 0;

    function setConnection(state, text) {
        connBadge.className = 'conn ' + state;
        connText.textContent = text;
    }

    function connect() {
        const scheme = location.protocol === 'https:' ? 'wss://' : 'ws://';
        setConnection('connecting', attempt ? 'reconnecting' : 'connecting');
        socket = new WebSocket(scheme + location.host + '/ws/live');

        socket.onopen = () => {
            attempt = 0;
            setConnection('live', 'live');
        };

        socket.onmessage = event => {
            lastFrameAt = Date.now();
            setConnection('live', 'live');
            try {
                render(JSON.parse(event.data));
            } catch (e) {
                // One malformed frame must not stop the map. Same argument as the consumer loop
                // on the server: a render that throws would leave onmessage dead for the life of
                // the socket, and the page would look connected while never updating again.
                console.error('bad frame', e);
            }
        };

        socket.onerror = () => socket.close();

        socket.onclose = () => {
            /*
             * Exponential backoff, capped at 15 seconds.
             *
             * The server going down is exactly when every open tab decides to reconnect at once.
             * Retrying immediately in a loop turns one restart into a stampede against a process
             * that is still starting up. Backing off costs one user a few seconds and costs the
             * server nothing.
             */
            const delay = Math.min(15000, 500 * Math.pow(2, attempt++));
            setConnection('down', 'offline · retry ' + Math.round(delay / 1000) + 's');
            setTimeout(connect, delay);
        };
    }

    /*
     * A TCP connection can be dead without anyone saying so - a laptop lid closing, a NAT dropping
     * the flow - and the socket sits there looking open. Frames arrive every second, so silence for
     * five is a real signal, and saying "stale" is more honest than a green light over a frozen map.
     */
    setInterval(() => {
        if (socket && socket.readyState === WebSocket.OPEN && lastFrameAt
                && Date.now() - lastFrameAt > 5000) {
            setConnection('connecting', 'stale');
        }
    }, 2000);

    connect();

    // ---------------------------------------------------------------- formatting

    function label(status) {
        return status ? status.toLowerCase().replace(/_/g, ' ') : 'unknown';
    }

    function distance(metres) {
        return metres >= 1000 ? (metres / 1000).toFixed(1) + ' km' : Math.round(metres) + ' m';
    }

    function duration(seconds) {
        if (seconds == null) return '—';
        if (seconds < 60) return seconds + 's';
        const minutes = Math.floor(seconds / 60);
        return minutes + 'm ' + (seconds % 60) + 's';
    }

    function ago(iso) {
        const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
        return duration(seconds);
    }

    /** The feed is third-party text going into innerHTML. Escape it. */
    function esc(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }
})();
