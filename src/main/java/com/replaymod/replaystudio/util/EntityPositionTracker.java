/*
 * Copyright (c) 2021
 *
 * This file is part of ReplayStudio.
 *
 * ReplayStudio is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ReplayStudio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ReplayStudio.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.replaymod.replaystudio.util;

import com.github.steveice10.packetlib.io.NetInput;
import com.github.steveice10.packetlib.io.NetOutput;
import com.github.steveice10.packetlib.io.stream.StreamNetInput;
import com.github.steveice10.packetlib.io.stream.StreamNetOutput;
import com.replaymod.replaystudio.lib.guava.base.Optional;
import com.replaymod.replaystudio.PacketData;
import com.replaymod.replaystudio.io.ReplayInputStream;
import com.replaymod.replaystudio.lib.viaversion.api.protocol.packet.State;
import com.replaymod.replaystudio.protocol.Packet;
import com.replaymod.replaystudio.protocol.PacketTypeRegistry;
import com.replaymod.replaystudio.replay.ReplayFile;
import com.replaymod.replaystudio.replay.ReplayMetaData;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * An EntityPositionTracker knows every entity's position at any timestamp for a single Replay.
 * To do so, it once reads the whole Replay and stores all packets that set or change an entity's position into memory.<br>
 *     <br>
 * While this significantly increases the Replay loading time, it's the only way to:<br>
 *     1) Properly preview the Camera Path when an Entity is spectated<br>
 *     2) Calculate a smooth Path from an Entity's Shoulder Cam perspective<br>
 * Instances of this class should therefore only be initialized when needed.
 * Results are also cached in the Replay file.<br>
 * <br>
 * This class is thread-safe. As such, it will synchronize on the ReplayFile object when using it.
 */
public class EntityPositionTracker {
    private static final String CACHE_ENTRY = "entity_positions.bin";
    private static final String OLD_CACHE_ENTRY = "entity_positions.json";

    private final ReplayFile replayFile;

    private volatile Map<Integer, PositionTimeline> entityPositions;

    public EntityPositionTracker(ReplayFile replayFile) {
        this.replayFile = replayFile;
    }

    /**
     * Load the entity positions either from cache or from the packet data.
     * @param progressMonitor Called with the current progress [0, 1] or not at all
     * @throws IOException if an i/o error occurs
     */
    public void load(Consumer<Double> progressMonitor) throws IOException {
        load(progressMonitor, () -> false);
    }

    /**
     * Load the entity positions either from cache or from the packet data.
     * @param progressMonitor Called with the current progress [0, 1] or not at all
     * @param cancelled checked regularly to abort long-running scans/cache writes
     * @throws IOException if an i/o error occurs
     */
    public void load(Consumer<Double> progressMonitor, BooleanSupplier cancelled) throws IOException {
        Optional<InputStream> cached;
        synchronized (replayFile) {
            throwIfCancelled(cancelled);
            Optional<InputStream> oldCache = replayFile.get(OLD_CACHE_ENTRY);
            if (oldCache.isPresent()) {
                oldCache.get().close();
                replayFile.remove(OLD_CACHE_ENTRY);
            }
            cached = replayFile.getCache(CACHE_ENTRY);
        }
        if (cached.isPresent()) {
            try (InputStream in = cached.get()) {
                loadFromCache(in, cancelled);
            } catch (EOFException e) {
                // Cache contains insufficient data, probably due to a previous crash / full disk
                loadFromPacketData(progressMonitor, cancelled);
                synchronized (replayFile) {
                    throwIfCancelled(cancelled);
                    replayFile.removeCache(CACHE_ENTRY);
                }
                saveToCache(cancelled);
            }
        } else {
            loadFromPacketData(progressMonitor, cancelled);
            saveToCache(cancelled);
        }
    }

    private void loadFromCache(InputStream rawIn, BooleanSupplier cancelled) throws IOException {
        NetInput in = new StreamNetInput(rawIn);
        Map<Integer, PositionTimeline> entityPositions = new HashMap<>();
        for (int i = in.readVarInt(); i > 0; i--) {
            throwIfCancelled(cancelled);
            int entityId = in.readVarInt();
            PositionTimeline.Builder timeline = new PositionTimeline.Builder();
            long time = 0;
            for (int j = in.readVarInt(); j > 0; j--) {
                throwIfCancelled(cancelled);
                time += in.readVarLong();
                timeline.append(time, new Location(
                        in.readDouble(), in.readDouble(), in.readDouble(), in.readFloat(), in.readFloat()
                ));
            }
            entityPositions.put(entityId, timeline.build());
        }
        this.entityPositions = entityPositions;
    }

    private void saveToCache(BooleanSupplier cancelled) throws IOException {
        synchronized (replayFile) {
            throwIfCancelled(cancelled);
            Optional<InputStream> cached = replayFile.getCache(CACHE_ENTRY);
            if (cached.isPresent()) {
                // Someone was faster than we were
                cached.get().close();
                return;
            }

            try (OutputStream rawOut = replayFile.writeCache(CACHE_ENTRY)) {
                NetOutput out = new StreamNetOutput(rawOut);
                out.writeVarInt(entityPositions.size());
                for (Map.Entry<Integer, PositionTimeline> entry : entityPositions.entrySet()) {
                    throwIfCancelled(cancelled);
                    out.writeVarInt(entry.getKey());
                    PositionTimeline timeline = entry.getValue();
                    out.writeVarInt(timeline.size());
                    long time = 0;
                    for (int i = 0; i < timeline.size(); i++) {
                        throwIfCancelled(cancelled);
                        long locTime = timeline.timeAt(i);
                        out.writeVarLong(locTime - time);
                        time = locTime;
                        out.writeDouble(timeline.xAt(i));
                        out.writeDouble(timeline.yAt(i));
                        out.writeDouble(timeline.zAt(i));
                        out.writeFloat(timeline.yawAt(i));
                        out.writeFloat(timeline.pitchAt(i));
                    }
                }
            }
        }
    }

    private void loadFromPacketData(Consumer<Double> progressMonitor, BooleanSupplier cancelled) throws IOException {
        // Get the packet data input stream
        int replayLength;
        ReplayInputStream origIn;
        synchronized (replayFile) {
            throwIfCancelled(cancelled);
            ReplayMetaData metaData = replayFile.getMetaData();
            replayLength = Math.max(1, metaData.getDuration());
            origIn = replayFile.getPacketData(PacketTypeRegistry.get(metaData.getProtocolVersion(), State.LOGIN));
        }

        // PLAN: MEM-01/MEM-02 avoid TreeMap<Long, Location> and store timelines as primitive arrays.
        // Replay packet timestamps are monotonic, so append-only builders preserve floor/higher lookup via binary search.
        Map<Integer, PositionTimeline.Builder> builders = new HashMap<>();
        int lastProgressPercent = -1;
        try (ReplayInputStream in = origIn) {
            PacketData packetData;
            while ((packetData = in.readPacket()) != null) {
                throwIfCancelled(cancelled);
                Packet packet = packetData.getPacket();

                Integer entityID = PacketUtils.getEntityId(packet);
                if (entityID == null) {
                    packet.release();
                    continue;
                }

                PositionTimeline.Builder positions = builders.get(entityID);
                if (positions == null) {
                    builders.put(entityID, positions = new PositionTimeline.Builder());
                }

                Location oldPosition = positions.lastLocation();
                Location newPosition = PacketUtils.updateLocation(oldPosition, packet);

                if (newPosition != null) {
                    positions.append(packetData.getTime(), newPosition);

                    int progressPercent = (int) Math.min(100, Math.max(0,
                            packetData.getTime() * 100L / replayLength));
                    if (progressPercent != lastProgressPercent) {
                        lastProgressPercent = progressPercent;
                        progressMonitor.accept(progressPercent / 100.0);
                    }
                }

                packet.release();
            }
        }
        throwIfCancelled(cancelled);
        progressMonitor.accept(1.0);

        Map<Integer, PositionTimeline> entityPositions = new HashMap<>(builders.size());
        for (Map.Entry<Integer, PositionTimeline.Builder> entry : builders.entrySet()) {
            throwIfCancelled(cancelled);
            entityPositions.put(entry.getKey(), entry.getValue().build());
        }
        this.entityPositions = entityPositions;
    }

    private static void throwIfCancelled(BooleanSupplier cancelled) throws InterruptedIOException {
        if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) {
            InterruptedIOException e = new InterruptedIOException("Entity position tracking was cancelled.");
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /**
     * @param entityID The ID of the entity
     * @param timestamp The timestamp
     * @return The position of the specified entity at the given timestamp
     *          or {@code null} if the entity hasn't yet been spawned at that timestamp
     * @throws IllegalStateException if {@link #load(Consumer)} hasn't been called or hasn't finished yet.
     */
    public Location getEntityPositionAtTimestamp(int entityID, long timestamp) {
        if (entityPositions == null) {
            throw new IllegalStateException("Not yet initialized.");
        }

        PositionTimeline positions = entityPositions.get(entityID);
        if (positions == null) {
            return null;
        }
        int lower = positions.floorIndex(timestamp);
        int higher = positions.higherIndex(timestamp);
        if (lower < 0 || higher < 0) {
            return null;
        }
        double r = (positions.timeAt(higher) - timestamp) / (double) (positions.timeAt(higher) - positions.timeAt(lower));
        return new Location(
                positions.xAt(lower) + (positions.xAt(higher) - positions.xAt(lower)) * r,
                positions.yAt(lower) + (positions.yAt(higher) - positions.yAt(lower)) * r,
                positions.zAt(lower) + (positions.zAt(higher) - positions.zAt(lower)) * r,
                positions.yawAt(lower) + (positions.yawAt(higher) - positions.yawAt(lower)) * (float) r,
                positions.pitchAt(lower) + (positions.pitchAt(higher) - positions.pitchAt(lower)) * (float) r
        );
    }

    private static final class PositionTimeline {
        private final long[] times;
        private final double[] xs;
        private final double[] ys;
        private final double[] zs;
        private final float[] yaws;
        private final float[] pitches;

        private PositionTimeline(long[] times, double[] xs, double[] ys, double[] zs, float[] yaws, float[] pitches) {
            this.times = times;
            this.xs = xs;
            this.ys = ys;
            this.zs = zs;
            this.yaws = yaws;
            this.pitches = pitches;
        }

        int size() {
            return times.length;
        }

        long timeAt(int index) {
            return times[index];
        }

        double xAt(int index) {
            return xs[index];
        }

        double yAt(int index) {
            return ys[index];
        }

        double zAt(int index) {
            return zs[index];
        }

        float yawAt(int index) {
            return yaws[index];
        }

        float pitchAt(int index) {
            return pitches[index];
        }

        int floorIndex(long timestamp) {
            int index = Arrays.binarySearch(times, timestamp);
            return index >= 0 ? index : -index - 2;
        }

        int higherIndex(long timestamp) {
            int index = Arrays.binarySearch(times, timestamp);
            if (index >= 0) {
                index++;
            } else {
                index = -index - 1;
            }
            return index < times.length ? index : -1;
        }

        private static final class Builder {
            private int size;
            private long[] times = new long[64];
            private double[] xs = new double[64];
            private double[] ys = new double[64];
            private double[] zs = new double[64];
            private float[] yaws = new float[64];
            private float[] pitches = new float[64];
            private Location lastLocation;

            Location lastLocation() {
                return lastLocation;
            }

            void append(long time, Location location) {
                if (size > 0 && times[size - 1] == time) {
                    set(size - 1, time, location);
                    return;
                }
                ensureCapacity(size + 1);
                set(size++, time, location);
            }

            PositionTimeline build() {
                return new PositionTimeline(
                        Arrays.copyOf(times, size),
                        Arrays.copyOf(xs, size),
                        Arrays.copyOf(ys, size),
                        Arrays.copyOf(zs, size),
                        Arrays.copyOf(yaws, size),
                        Arrays.copyOf(pitches, size)
                );
            }

            private void set(int index, long time, Location location) {
                times[index] = time;
                xs[index] = location.getX();
                ys[index] = location.getY();
                zs[index] = location.getZ();
                yaws[index] = location.getYaw();
                pitches[index] = location.getPitch();
                lastLocation = location;
            }

            private void ensureCapacity(int capacity) {
                if (capacity <= times.length) {
                    return;
                }
                int newCapacity = Math.max(capacity, times.length * 2);
                times = Arrays.copyOf(times, newCapacity);
                xs = Arrays.copyOf(xs, newCapacity);
                ys = Arrays.copyOf(ys, newCapacity);
                zs = Arrays.copyOf(zs, newCapacity);
                yaws = Arrays.copyOf(yaws, newCapacity);
                pitches = Arrays.copyOf(pitches, newCapacity);
            }
        }
    }
}
