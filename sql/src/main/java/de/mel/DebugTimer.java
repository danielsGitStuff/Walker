package de.mel;

/**
 * Helps finding methods wasting valuable runtime. Has fewer nasty side effects than profiling.
 */
public class DebugTimer {

    private final String name;

    private long sum;
    private long startTime;
    private long fps = 0;
    private long startCount = 0;

    public DebugTimer(String name) {
        this.name = name;
    }

    public DebugTimer start() {
        startTime = System.nanoTime();
        startCount++;
        return this;
    }

    public long fps() {
        long duration = (System.currentTimeMillis() - fps);
        fps = System.currentTimeMillis();
        if (duration > 0)
            return 1000 / duration;
        return 0;
    }

    @Override
    public String toString() {
        return "Timer[" + name + "]: " + getDurationInMS() + "ms";
    }

    public DebugTimer stop() {
        sum = System.nanoTime() - startTime + sum;
        return this;
    }

    public long getDurationInMS() {
        return (sum / 1000000);
    }

    public long getDurationInNS() {
        return sum;
    }

    public long getDurationInS() {
        return ((System.nanoTime() - startTime) / 1000000000);
    }

    public DebugTimer  print() {
        System.out.println(this.getClass().getSimpleName() + ".'" + name + "'.print: " + sum / 1000000 + " ms");
        return this;
    }

    public DebugTimer reset() {
        sum = 0;
        startCount = 0;
        return this;
    }

    public long getStartCount() {
        return startCount;
    }

    public long getAverageDuration() {
        return sum / startCount;
    }
}
