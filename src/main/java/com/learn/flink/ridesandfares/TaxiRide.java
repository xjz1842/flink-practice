package com.learn.flink.ridesandfares;


import com.learn.flink.ridesandfares.util.DataGenerator;
import com.learn.flink.ridesandfares.util.GeoUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;

public class TaxiRide implements Comparable<TaxiRide>, Serializable {

    /**
     * Creates a new TaxiRide with now as start and end time.
     */
    public TaxiRide() {
        this.startTime = Instant.now();
        this.endTime = Instant.now();
    }

    /**
     * Invents a TaxiRide.
     */
    public TaxiRide(long rideId, boolean isStart) {
        DataGenerator g = new DataGenerator(rideId);

        this.rideId = rideId;
        this.isStart = isStart;
        this.startTime = g.startTime();
        this.endTime = isStart ? Instant.ofEpochMilli(0) : g.endTime();
        this.startLon = g.startLon();
        this.startLat = g.startLat();
        this.endLon = g.endLon();
        this.endLat = g.endLat();
        this.passengerCnt = g.passengerCnt();
        this.taxiId = g.taxiId();
        this.driverId = g.driverId();
    }

    /**
     * Creates a TaxiRide with the given parameters.
     */
    public TaxiRide(long rideId, boolean isStart, Instant startTime, Instant endTime,
                    float startLon, float startLat, float endLon, float endLat,
                    short passengerCnt, long taxiId, long driverId) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startLon = startLon;
        this.startLat = startLat;
        this.endLon = endLon;
        this.endLat = endLat;
        this.passengerCnt = passengerCnt;
        this.taxiId = taxiId;
        this.driverId = driverId;
    }

    public long rideId;
    public boolean isStart;
    public Instant startTime;
    public Instant endTime;
    public float startLon;
    public float startLat;
    public float endLon;
    public float endLat;
    public short passengerCnt;
    public long taxiId;
    public long driverId;

    @Override
    public String toString() {

        return rideId + "," +
                (isStart ? "START" : "END") + "," +
                startTime.toString() + "," +
                endTime.toString() + "," +
                startLon + "," +
                startLat + "," +
                endLon + "," +
                endLat + "," +
                passengerCnt + "," +
                taxiId + "," +
                driverId;
    }

    /**
     * Compares this TaxiRide with the given one.
     *
     * <ul>
     *     <li>sort by timestamp,</li>
     *     <li>putting START events before END events if they have the same timestamp</li>
     * </ul>
     */
    public int compareTo(@Nullable TaxiRide other) {
        if (other == null) {
            return 1;
        }
        int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
        if (compareTimes == 0) {
            if (this.isStart == other.isStart) {
                return 0;
            } else {
                if (this.isStart) {
                    return -1;
                } else {
                    return 1;
                }
            }
        } else {
            return compareTimes;
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiRide &&
                this.rideId == ((TaxiRide) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int) this.rideId;
    }

    /**
     * Gets the ride's time stamp (start or end time depending on {@link #isStart}).
     */
    public long getEventTime() {
        if (isStart) {
            return startTime.toEpochMilli();
        } else {
            return endTime.toEpochMilli();
        }
    }

    /**
     * Gets the distance from the ride location to the given one.
     */
    public double getEuclideanDistance(double longitude, double latitude) {
        if (this.isStart) {
            return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.startLon, this.startLat);
        } else {
            return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.endLon, this.endLat);
        }
    }
}
