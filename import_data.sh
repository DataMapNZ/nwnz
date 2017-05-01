#!/bin/bash

#SETUP
PSQL_BINARY=`which psql`
DATABASE_NAME='nature_watch_dwca'
SCHEMA='public'
DATABASE_USER='nwnz'
# Put additional connections information here like host, port etc
# for local users this should be sufficient
PG_CONNECTION_OPTIONS="-U ${DATABASE_USER} -h localhost "
PSQL="${PSQL_BINARY} ${PG_CONNECTION_OPTIONS}"

MY_DIRECTORY="$( cd "$( dirname "$0" )" && pwd )"
DATA_DIRECTORY="${MY_DIRECTORY}/naturewatch-observations-dwca"
IMAGES_FILE="${DATA_DIRECTORY}/images.csv"
OBSERVATIONS_FILE="${DATA_DIRECTORY}/observations.csv"
OBSERVATION_FIELDS_FILE="${DATA_DIRECTORY}/observation_fields.csv"
PROJECT_OBSERVATIONS_FILE="${DATA_DIRECTORY}/project_observations.csv"


#--------------- From here on out there is a ton of SQL
#--------------- It may be better to split these up into multiple files later

# DOWNLOAD THE DATA FILE FROM INATURALIST
rm -f naturewatch-observations-dwca.zip
wget http://inaturalist.org/observations/naturewatch-observations-dwca.zip

# EXTRACT THE DATA FILES
if [ ! -d $DATA_DIRECTORY ] ; then
    mkdir $DATA_DIRECTORY
fi
rm -f $DATA_DIRECTORY/*
unzip -d $DATA_DIRECTORY naturewatch-observations-dwca.zip


# CREATE THE DATABASE IF IT DOES NOT EXISTS
${PSQL} -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '${DATABASE_NAME}'" | grep -q 1 || ${PSQL} -d postgres -c "CREATE DATABASE ${DATABASE_NAME}"

#Append the database to our command for future queries
run_query="${PSQL} -d ${DATABASE_NAME} -c "

echo $run_query
echo $OBSERVATIONS_FILE

# set up some extensions
$run_query "create extension if not exists hstore;
            create extension if not exists postgis"


# Create the base tables and import them
# These are going to be unlogged tables because I am treating them as temp tables
# unlogged should speed up things a but if the database crashes mid run
# you'll probably lose some records so just rerun the whole thing
# They will be used to create the final master tables


#import observations
# NOTE:  I changed the "references" and the "order" field names,
#        both of these are reserved keywords they will be renamed in the
#        final table to something more sensible


$run_query "
            DROP TABLE if exists ${SCHEMA}.observations cascade;

            CREATE  TABLE ${SCHEMA}.observations
            (
              id bigint NOT NULL,
              occurrence_id text,
              basis_of_record text,
              modified timestamp with time zone,
              institution_code text,
              collection_code text,
              dataset_name text,
              information_withheld text,
              catalog_number bigint,
              _references text,
              occurrence_remarks text,
              occurrence_details text,
              recorded_by text,
              establishment_means text,
              event_date date,
              event_time time without time zone,
              verbatim_event_date text,
              verbatim_locality text,
              decimal_latitude numeric(13,10),
              decimal_longitude numeric(13,10),
              coordinate_uncertainty_in_meters integer,
              countrycode text,
              identification_id bigint,
              date_identified timestamp with time zone,
              identification_remarks text,
              taxon_id bigint,
              scientific_name text,
              taxon_rank text,
              kingdom text,
              phylum text,
              class text,
              _order text,
              family text,
              genus text,
              license text,
              rights text,
              rights_holder text,
              CONSTRAINT ${SCHEMA}_observations_pkey PRIMARY KEY (id)
            );"

$run_query "COPY ${SCHEMA}.observations FROM STDIN WITH CSV HEADER" < $OBSERVATIONS_FILE



#  Create images table
#
$run_query  "DROP TABLE if exists ${SCHEMA}.images CASCADE;

            CREATE  TABLE ${SCHEMA}.images
            (
              observation_id bigint,
              image_type text,
              format text,
              identifier text,
              url text,
              created timestamp with time zone,
              creator text,
              publisher text,
              license text,
              rights_holder text
            );"

#copy it
$run_query "COPY ${SCHEMA}.images FROM STDIN WITH CSV HEADER" < $IMAGES_FILE

#create INDEX
$run_query "CREATE INDEX ${SCHEMA}_images_observation_id ON ${SCHEMA}.images USING btree (observation_id);"

# create observation_fields table

$run_query "DROP TABLE if exists ${SCHEMA}.observation_fields CASCADE;

CREATE TABLE ${SCHEMA}.observation_fields
(
  observation_id bigint,
  identifier bigint NOT NULL,
  fieldname text,
  fieldid text,
  value text,
  datatype text,
  created timestamp with time zone,
  modified timestamp with time zone,
  CONSTRAINT ${SCHEMA}_observation_fields_pkey PRIMARY KEY (identifier),
  CONSTRAINT ${SCHEMA}_observation_fields_observation_id_fkey FOREIGN KEY (observation_id)
      REFERENCES ${SCHEMA}.observations (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);"


#copy it
$run_query "COPY ${SCHEMA}.observation_fields FROM STDIN WITH CSV HEADER" < $OBSERVATION_FIELDS_FILE

#create FK INDEX
$run_query "CREATE INDEX ${SCHEMA}_observation_fields_on_observation_id ON ${SCHEMA}.observation_fields USING btree (observation_id);"

#Project observations

$run_query "DROP TABLE if exists ${SCHEMA}.project_observations CASCADE;

            CREATE TABLE ${SCHEMA}.project_observations
            (
              observation_id bigint,
              identifier bigint NOT NULL,
              project_uri text,
              project_title text,
              created timestamp with time zone,
              modified timestamp with time zone,
              CONSTRAINT ${SCHEMA}_project_observations_pkey PRIMARY KEY (identifier)
            );"

#copy it
$run_query "COPY ${SCHEMA}.project_observations FROM STDIN WITH CSV HEADER" < $PROJECT_OBSERVATIONS_FILE

#create FK INDEX
$run_query "CREATE INDEX ${SCHEMA}_project_observations_on_observation_id ON ${SCHEMA}.project_observations USING btree (observation_id);"

# Now we create a flat table with all the information from the above tables
$run_query "DROP TABLE IF EXISTS ${SCHEMA}.observations_complete CASCADE;

          create table ${SCHEMA}.observations_complete as
              select ob.id
          		,ob.occurrence_id
          		,ob.basis_of_record
          		,ob.modified
          		,ob.institution_code
          		,ob.collection_code
          		,ob.dataset_name
          		,ob.information_withheld
          		,ob.catalog_number
          		,ob._references as references_uri
          		,ob.occurrence_remarks
          		,ob.occurrence_details
          		,ob.recorded_by
          		,ob.establishment_means
          		,ob.event_date
          		,ob.event_time
          		,ob.verbatim_event_date
          		,ob.verbatim_locality
          		,ob.decimal_latitude
          		,ob.decimal_longitude
          		,ob.coordinate_uncertainty_in_meters
          		,ob.countrycode
          		,ob.identification_id
          		,ob.date_identified
          		,ob.identification_remarks
          		,ob.taxon_id,ob.scientific_name
          		,ob.taxon_rank
          		,ob.kingdom
          		,ob.phylum
          		,ob.class
          		,ob._order as \"order\"
          		,ob.family
          		,ob.genus
          		,ob.license
          		,ob.rights
          		,ob.rights_holder
                ,ST_SetSRID(ST_MakePoint(ob.decimal_longitude, ob.decimal_latitude),4326)::geometry(POINT,4326) as geom
                ,ST_Shift_longitude(ST_SetSRID(ST_MakePoint(ob.decimal_longitude, ob.decimal_latitude),4326)::geometry(POINT,4326)) as geom_360
                ,im.images_json
          		,obf.observation_fields_hstore
          		,obf.observation_fields_json
          		,po.project_uri
          		,po.project_title
          		,(split_part(po.project_uri, '/', 5))::integer as project_id
          FROM ${SCHEMA}.observations ob
          LEFT JOIN (
                -- Doing a join on a sub select in order to get around a json_agg quirk
          		SELECT
          			observation_id
          			,json_agg(row_to_json(images.*)) as images_json
          		FROM ${SCHEMA}.images group by 1
          		) im on ob.id = im.observation_id
          LEFT JOIN (
          		select observation_id
          		,hstore(
          			array_agg(fieldname),array_agg(value)
          			) observation_fields_hstore
          			,json_agg(json_build_object(fieldname, value)) observation_fields_json
          		FROM ${SCHEMA}.observation_fields group by observation_id
          		) obf on ob.id = obf.observation_id
          left join ${SCHEMA}.project_observations po on ob.id = po.observation_id;"
