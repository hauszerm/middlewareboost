function AvgIntervalDate(timestamp, interval) {
    dtTimestamp = new Date(timestamp);

    var area = Math.floor(dtTimestamp.getMinutes() / interval);
    dtTimestamp.setMinutes((area + 1) * interval, 0, 0);

    return dtTimestamp.toISOString();
}