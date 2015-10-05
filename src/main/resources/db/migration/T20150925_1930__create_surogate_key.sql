-- Create a new column with artificial surrogate key

ALTER TABLE transtats ADD COLUMN flight_id SERIAL UNIQUE;
ALTER TABLE transtats ADD PRIMARY KEY (flight_id);