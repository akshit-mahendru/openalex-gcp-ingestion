-- sql/schema/01_initialize_schema.sql
-- Initialize OpenAlex Schema
-- This script creates the complete database schema for OpenAlex data

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS openalex;

-- Set search path
SET search_path TO openalex, public;

-- Create required extensions
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Drop existing tables if they exist (careful with this in production)
DROP TABLE IF EXISTS concepts CASCADE;
DROP TABLE IF EXISTS concepts_ancestors CASCADE;
DROP TABLE IF EXISTS concepts_related_concepts CASCADE;
DROP TABLE IF EXISTS concepts_counts_by_year CASCADE;
DROP TABLE IF EXISTS concepts_ids CASCADE;
DROP TABLE IF EXISTS works CASCADE;
DROP TABLE IF EXISTS works_biblio CASCADE;
DROP TABLE IF EXISTS works_concepts CASCADE;
DROP TABLE IF EXISTS works_ids CASCADE;
DROP TABLE IF EXISTS works_open_access CASCADE;
DROP TABLE IF EXISTS works_mesh CASCADE;
DROP TABLE IF EXISTS works_alternate_host_venues CASCADE;
DROP TABLE IF EXISTS works_authorships CASCADE;
DROP TABLE IF EXISTS works_related_works CASCADE;
DROP TABLE IF EXISTS works_referenced_works CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS authors_ids CASCADE;
DROP TABLE IF EXISTS authors_counts_by_year CASCADE;
DROP TABLE IF EXISTS venues CASCADE;
DROP TABLE IF EXISTS venues_ids CASCADE;
DROP TABLE IF EXISTS venues_counts_by_year CASCADE;
DROP TABLE IF EXISTS institutions CASCADE;
DROP TABLE IF EXISTS institutions_ids CASCADE;
DROP TABLE IF EXISTS institutions_associated_institutions CASCADE;
DROP TABLE IF EXISTS institutions_counts_by_year CASCADE;
DROP TABLE IF EXISTS institutions_geo CASCADE;

-- Create tables for Concepts
CREATE TABLE concepts (
    id text PRIMARY KEY,
    wikidata text,
    display_name text,
    level int4,
    description text,
    works_count int4,
    cited_by_count int4,
    image_url text,
    image_thumbnail_url text,
    works_api_url text,
    updated_date timestamp
);

CREATE TABLE concepts_ancestors (
    concept_id text,
    ancestor_id text,
    PRIMARY KEY (concept_id, ancestor_id),
    FOREIGN KEY (concept_id) REFERENCES concepts(id),
    FOREIGN KEY (ancestor_id) REFERENCES concepts(id)
);

CREATE TABLE concepts_related_concepts (
    concept_id text,
    related_concept_id text,
    score float4,
    PRIMARY KEY (concept_id, related_concept_id),
    FOREIGN KEY (concept_id) REFERENCES concepts(id),
    FOREIGN KEY (related_concept_id) REFERENCES concepts(id)
);

CREATE TABLE concepts_counts_by_year (
    concept_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (concept_id, year),
    FOREIGN KEY (concept_id) REFERENCES concepts(id)
);

CREATE TABLE concepts_ids (
    concept_id text PRIMARY KEY,
    openalex text,
    wikidata text,
    wikipedia text,
    umls_aui text,
    umls_cui text,
    mag int8,
    FOREIGN KEY (concept_id) REFERENCES concepts(id)
);

-- Create tables for Works
CREATE TABLE works (
    id text PRIMARY KEY,
    doi text,
    title text,
    display_name text,
    publication_year int4,
    publication_date text,
    type text,
    cited_by_count int4,
    is_retracted boolean,
    is_paratext boolean,
    host_venue text
);

CREATE TABLE works_biblio (
    work_id text PRIMARY KEY,
    volume text,
    issue text,
    first_page text,
    last_page text,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_concepts (
    work_id text,
    concept_id text,
    score float4,
    PRIMARY KEY (work_id, concept_id),
    FOREIGN KEY (work_id) REFERENCES works(id),
    FOREIGN KEY (concept_id) REFERENCES concepts(id)
);

CREATE TABLE works_ids (
    work_id text PRIMARY KEY,
    openalex text,
    doi text,
    pmid text,
    pmcid text,
    mag int8,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_mesh (
    work_id text,
    descriptor_ui text,
    descriptor_name text,
    qualifier_ui text,
    qualifier_name text,
    PRIMARY KEY (work_id, descriptor_ui, qualifier_ui),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_open_access (
    work_id text PRIMARY KEY,
    is_oa boolean,
    oa_status text,
    oa_url text,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_alternate_host_venues (
    work_id text,
    venue_id text,
    PRIMARY KEY (work_id, venue_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_related_works (
    work_id text,
    related_work_id text,
    PRIMARY KEY (work_id, related_work_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_referenced_works (
    work_id text,
    referenced_work_id text,
    PRIMARY KEY (work_id, referenced_work_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

-- Create tables for Authors
CREATE TABLE authors (
    id text PRIMARY KEY,
    orcid text,
    display_name text,
    display_name_alternatives text[],
    works_count int4,
    cited_by_count int4,
    last_known_institution text,
    works_api_url text,
    updated_date timestamp
);

CREATE TABLE authors_ids (
    author_id text PRIMARY KEY,
    openalex text,
    orcid text,
    scopus text,
    twitter text,
    wikipedia text,
    mag int8,
    FOREIGN KEY (author_id) REFERENCES authors(id)
);

CREATE TABLE authors_counts_by_year (
    author_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (author_id, year),
    FOREIGN KEY (author_id) REFERENCES authors(id)
);

-- Create tables for Venues
CREATE TABLE venues (
    id text PRIMARY KEY,
    issn_l text,
    issn text[],
    display_name text,
    publisher text,
    works_count int4,
    cited_by_count int4,
    is_in_doaj boolean,
    is_oa boolean,
    homepage_url text,
    works_api_url text,
    updated_date timestamp
);

CREATE TABLE venues_ids (
    venue_id text PRIMARY KEY,
    openalex text,
    issn_l text,
    issn text[],
    mag int8,
    FOREIGN KEY (venue_id) REFERENCES venues(id)
);

CREATE TABLE venues_counts_by_year (
    venue_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (venue_id, year),
    FOREIGN KEY (venue_id) REFERENCES venues(id)
);

-- Create tables for Institutions
CREATE TABLE institutions (
    id text PRIMARY KEY,
    ror text,
    display_name text,
    country_code text,
    type text,
    homepage_url text,
    image_url text,
    image_thumbnail_url text,
    display_name_acronyms text[],
    display_name_alternatives text[],
    works_count int4,
    cited_by_count int4,
    works_api_url text,
    updated_date timestamp
);

CREATE TABLE institutions_ids (
    institution_id text PRIMARY KEY,
    openalex text,
    ror text,
    grid text,
    wikipedia text,
    wikidata text,
    mag int8,
    FOREIGN KEY (institution_id) REFERENCES institutions(id)
);

CREATE TABLE institutions_associated_institutions (
    institution_id text,
    associated_institution_id text,
    relationship text,
    PRIMARY KEY (institution_id, associated_institution_id),
    FOREIGN KEY (institution_id) REFERENCES institutions(id),
    FOREIGN KEY (associated_institution_id) REFERENCES institutions(id)
);

CREATE TABLE institutions_counts_by_year (
    institution_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (institution_id, year),
    FOREIGN KEY (institution_id) REFERENCES institutions(id)
);

CREATE TABLE institutions_geo (
    institution_id text PRIMARY KEY,
    city text,
    geonames_city_id text,
    region text,
    country_code text,
    country text,
    latitude float8,
    longitude float8,
    FOREIGN KEY (institution_id) REFERENCES institutions(id)
);

-- Create indexes for better query performance
CREATE INDEX idx_concepts_display_name ON concepts(display_name);
CREATE INDEX idx_works_doi ON works(doi);
CREATE INDEX idx_works_publication_year ON works(publication_year);
CREATE INDEX idx_works_type ON works(type);
CREATE INDEX idx_works_cited_by_count ON works(cited_by_count);
CREATE INDEX idx_authors_display_name ON authors(display_name);
CREATE INDEX idx_authors_orcid ON authors(orcid);
CREATE INDEX idx_venues_display_name ON venues(display_name);
CREATE INDEX idx_institutions_country_code ON institutions(country_code);
CREATE INDEX idx_institutions_type ON institutions(type);

-- Create GIN indexes for array and full-text search
CREATE INDEX idx_authors_display_name_alternatives_gin ON authors USING gin(display_name_alternatives);
CREATE INDEX idx_institutions_display_name_alternatives_gin ON institutions USING gin(display_name_alternatives);
CREATE INDEX idx_venues_issn_gin ON venues USING gin(issn);

-- Add comment to the schema
COMMENT ON SCHEMA openalex IS 'Schema for OpenAlex academic data warehouse';