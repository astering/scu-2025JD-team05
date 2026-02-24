USE bishe;

INSERT INTO aircraft_capacity (aircraft_type, aircraft_type_norm, max_people, source_file)
VALUES
-- Airbus
('空客350(大)',        '空客A350',      300, 'manual_fill'),
('空客330-300(大)',    '空客A330',      440, 'manual_fill'),
('空客321-200(中)',    '空客A321',      220, 'manual_fill'),
('空客32S(中)',        '空客A320',      180, 'manual_fill'),
('空客330(大)',        '空客A330',      440, 'manual_fill'),
-- Boeing
('波音738(中)',        '波音737-800',   180, 'manual_fill')

ON DUPLICATE KEY UPDATE
    max_people = GREATEST(max_people, VALUES(max_people)),
    aircraft_type_norm = VALUES(aircraft_type_norm),
    source_file = VALUES(source_file);

UPDATE aircraft_capacity
SET aircraft_type_norm = CASE
    -- Airbus widebody
    WHEN aircraft_type LIKE '空客380%'          THEN '空客A380'
    WHEN aircraft_type LIKE '空客350%'          THEN '空客A350'
    WHEN aircraft_type LIKE '空客330%'          THEN '空客A330'

    -- Airbus A32x
    WHEN aircraft_type LIKE '空客321%'          THEN '空客A321'
    WHEN aircraft_type LIKE '空客320%'          THEN '空客A320'
    WHEN aircraft_type LIKE '空客319%'          THEN '空客A319'
    WHEN aircraft_type LIKE '空客32S%'          THEN '空客A320'

    -- Boeing widebody
    WHEN aircraft_type LIKE '波音747%'          THEN '波音747'
    WHEN aircraft_type LIKE '波音777%'          THEN '波音777'
    WHEN aircraft_type LIKE '波音787%'          THEN '波音787'
    WHEN aircraft_type LIKE '波音767%'          THEN '波音767'
    WHEN aircraft_type LIKE '波音757%'          THEN '波音757'

    -- Boeing narrowbody
    WHEN aircraft_type LIKE '波音738%'          THEN '波音737-800'
    WHEN aircraft_type LIKE '波音737%'          THEN '波音737-800'

    -- Regional jet
    WHEN aircraft_type LIKE 'ERJ-190%'           THEN 'ERJ-190'
    WHEN aircraft_type LIKE '庞巴迪CRJ900%'     THEN 'CRJ900'
    WHEN aircraft_type LIKE 'ERJ(%'              THEN 'ERJ'
    WHEN aircraft_type LIKE 'CRJ(%'              THEN 'CRJ'

    -- Fallback
    WHEN aircraft_type = 'JET'                  THEN 'JET'
    WHEN aircraft_type = '其他机型'              THEN 'OTHER'

    ELSE aircraft_type
END
WHERE id IN (
    SELECT id FROM (
        SELECT id
        FROM aircraft_capacity
        WHERE aircraft_type_norm IS NULL
    ) t
);
