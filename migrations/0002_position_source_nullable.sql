-- OpenSky can omit position_source. It used to be stored as 0 (ADS-B), which
-- mislabels the report; store NULL instead. Existing rows are left as they are,
-- since a real 0 and a defaulted 0 can't be told apart.
ALTER TABLE state_vectors ALTER COLUMN position_source DROP NOT NULL;
