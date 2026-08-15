package dev.headway.gtfs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Builds a small GTFS zip in a temp directory.
 *
 * <p>Committing MARTA's real 21 MB archive as a fixture would bloat the repo and make the tests
 * depend on whatever service pattern happened to be running the day it was captured. A synthetic
 * archive is a few hundred bytes, and — more usefully — it can contain the awkward cases on
 * purpose: a comma inside a quoted field, a byte-order mark, a trip with no shape.
 */
final class GtfsZipBuilder {

    private final Map<String, String> files = new LinkedHashMap<>();
    private boolean withBom;

    GtfsZipBuilder file(String name, String content) {
        files.put(name, content);
        return this;
    }

    /** Prefixes every file with a UTF-8 BOM, as some agency exports do. */
    GtfsZipBuilder withByteOrderMark() {
        this.withBom = true;
        return this;
    }

    Path writeTo(Path directory) throws IOException {
        Path zip = directory.resolve("google_transit.zip");
        try (OutputStream out = Files.newOutputStream(zip);
             ZipOutputStream zos = new ZipOutputStream(out)) {
            for (Map.Entry<String, String> entry : files.entrySet()) {
                zos.putNextEntry(new ZipEntry(entry.getKey()));
                if (withBom) {
                    zos.write(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF});
                }
                zos.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        return zip;
    }

    /** A minimal but realistic feed shaped like MARTA's. */
    static GtfsZipBuilder martaLike() {
        return new GtfsZipBuilder()
                .file("routes.txt", """
                        route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
                        26903,MARTA,1,Joseph E. Lowery Blvd / 17th St,,3,https://itsmarta.com/1.aspx,9B3694,000000
                        26913,MARTA,15,"Clifton Road, Candler Road",,3,https://itsmarta.com/15.aspx,367AA8,000000
                        27457,MARTA,A,Rapid A Line,,1,,FF0000,FFFFFF
                        """)
                .file("trips.txt", """
                        route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,bikes_allowed
                        26903,3,10001,Arts Center Stn,,0,1217250,SHAPE_A,1,0
                        26903,3,10002,West End Stn,,1,1217251,SHAPE_B,1,0
                        26913,3,10003,Candler Road,,0,1217252,SHAPE_A,1,0
                        26913,3,10004,No Shape Here,,0,1217253,,1,0
                        26913,3,10005,Bad Direction,,7,1217254,SHAPE_A,1,0
                        """)
                // SHAPE_A: due north from the same origin, ~111.32 m per 0.001 degree of latitude.
                // shape_dist_traveled is in KILOMETRES, as MARTA publishes it.
                .file("shapes.txt", """
                        shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled
                        SHAPE_A,33.750000,-84.390000,1,0.0000
                        SHAPE_A,33.751000,-84.390000,2,0.1113
                        SHAPE_A,33.752000,-84.390000,3,0.2227
                        SHAPE_A,33.753000,-84.390000,4,0.3340
                        SHAPE_B,33.760000,-84.400000,1,
                        SHAPE_B,33.761000,-84.400000,2,
                        SHAPE_B,33.762000,-84.400000,3,
                        """);
    }
}
