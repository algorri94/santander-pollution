DROP KEYSPACE santanderPollution;

CREATE KEYSPACE santanderPollution WITH REPLICATION = {'class':'SimpleStrategy',
										 'replication_factor':1};
--  This is designed for a single node system so the replication is irrelevant

USE santanderPollution;

CREATE TABLE lecturas (
    sensor_id int,
    lat float,
    lon float,
    no2 int,
    ozone int,
    temp float,
    co float,
    particles float,
    region int,
    generated timestamp,
    timeframe timestamp,
    PRIMARY KEY (region, sensor_id, generated)
);

CREATE TABLE pollutionStats (
    description text,
    timeframe timestamp,
    region int,
    val counter,
    PRIMARY KEY ((region, timeframe), description)
        -- Every single batch that updates the counters updates all stats in the same region and timeframe.
        -- Therefore, the best partitioning strategy is putting them all together in the same partition.
);
