BEGIN;
CREATE SCHEMA IF NOT EXISTS video_detection_service;

DROP TABLE IF EXISTS video_detection_service.states;
CREATE TABLE IF NOT EXISTS video_detection_service.states(
            id SERIAL PRIMARY KEY,
            status VARCHAR,
            ulid_name VARCHAR,
            datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ); 
DROP TABLE IF EXISTS video_detection_service.predictions;
CREATE TABLE IF NOT EXISTS video_detection_service.predictions(
            id INTEGER,
            ulid_name VARCHAR,
            frame_id SERIAL PRIMARY KEY,
            detection_result JSONB,
            datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
COMMIT;