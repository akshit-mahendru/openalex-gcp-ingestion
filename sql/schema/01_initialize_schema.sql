-- sql/schema/01_initialize_schema.sql
-- Initialize OpenAlex Schema with Full Entity Definitions

-- Create schema and set up extensions
CREATE SCHEMA IF NOT EXISTS openalex;
SET search_path TO openalex, public;

-- Create required extensions
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Drop existing tables (use with caution in production)
DROP SCHEMA IF EXISTS openalex CASCADE;
CREATE SCHEMA openalex;
SET search_path TO openalex, public;

CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Works Table with Comprehensive Schema
CREATE TABLE works (
    id text PRIMARY KEY,
    doi text,
    title text,
    display_name text,
    publication_year int4,
    publication_date text,
    type text,
    cited_by_count int4,
    cited_by_api_url text,
    is_retracted boolean DEFAULT false,
    is_paratext boolean DEFAULT false,
    abstract_inverted_index jsonb,
    alternate_titles jsonb,
    cited_by_percentage float4,
    updated_date timestamp,
    created_date timestamp
);

-- Works Detailed Tables
CREATE TABLE works_ids (
    work_id text PRIMARY KEY,
    openalex text UNIQUE,
    doi text UNIQUE,
    mag int8 UNIQUE,
    pmid text UNIQUE,
    pmcid text UNIQUE,
    wikidata text UNIQUE,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_host_venue (
    work_id text PRIMARY KEY,
    venue_id text,
    url text,
    is_oa boolean,
    version text,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_open_access (
    work_id text PRIMARY KEY,
    is_oa boolean,
    oa_status text CHECK (oa_status IN ('gold', 'green', 'bronze', 'hybrid', 'closed')),
    oa_url text,
    is_official_oa boolean,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_authorships (
    work_id text,
    author_id text,
    author_position text CHECK (author_position IN ('first', 'middle', 'last')),
    institution_id text,
    raw_author_name text,
    primary_author boolean DEFAULT false,
    PRIMARY KEY (work_id, author_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_biblio (
    work_id text PRIMARY KEY,
    volume text,
    issue text,
    first_page text,
    last_page text,
    total_pages int4,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_concepts (
    work_id text,
    concept_id text,
    score float4,
    PRIMARY KEY (work_id, concept_id)
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

CREATE TABLE works_locations (
    work_id text,
    venue_id text,
    landing_page_url text,
    pdf_url text,
    is_oa boolean,
    source text,
    license text,
    version text CHECK (version IN ('submittedVersion', 'acceptedVersion', 'publishedVersion')),
    PRIMARY KEY (work_id, venue_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_related_works (
    work_id text,
    related_work_id text,
    relation_type text,
    PRIMARY KEY (work_id, related_work_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_referenced_works (
    work_id text,
    referenced_work_id text,
    PRIMARY KEY (work_id, referenced_work_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_counts_by_year (
    work_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (work_id, year),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

-- Indexes for performance
CREATE INDEX idx_works_doi ON works(doi);
CREATE INDEX idx_works_publication_year ON works(publication_year);
CREATE INDEX idx_works_type ON works(type);
CREATE INDEX idx_works_cited_by_count ON works(cited_by_count);
CREATE INDEX idx_works_title_gin ON works USING gin(to_tsvector('english', title));
CREATE INDEX idx_works_abstract_gin ON works USING gin(abstract_inverted_index);

-- Authors Tables Continuation

-- Authors Table
CREATE TABLE authors (
    id text PRIMARY KEY,
    orcid text,
    display_name text,
    display_name_alternatives jsonb,
    works_count int4,
    cited_by_count int4,
    last_known_institution text,
    works_api_url text,
    updated_date timestamp,
    created_date timestamp
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

CREATE TABLE authors_concepts (
    author_id text,
    concept_id text,
    score float4,
    PRIMARY KEY (author_id, concept_id),
    FOREIGN KEY (author_id) REFERENCES authors(id)
);

-- Indexes for Authors
CREATE INDEX idx_authors_display_name ON authors(display_name);
CREATE INDEX idx_authors_orcid ON authors(orcid);
CREATE INDEX idx_authors_last_known_institution ON authors(last_known_institution);
CREATE INDEX idx_authors_display_name_alternatives_gin ON authors USING gin(display_name_alternatives);

-- Sources (Journals/Repositories) Tables

CREATE TABLE sources (
    id text PRIMARY KEY,
    issn_l text,
    issn jsonb,
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

CREATE TABLE sources_ids (
    source_id text PRIMARY KEY,
    openalex text,
    issn_l text,
    issn jsonb,
    mag int8,
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE sources_counts_by_year (
    source_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (source_id, year),
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE sources_concepts (
    source_id text,
    concept_id text,
    score float4,
    PRIMARY KEY (source_id, concept_id),
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

-- Indexes for Sources
CREATE INDEX idx_sources_display_name ON sources(display_name);
CREATE INDEX idx_sources_publisher ON sources(publisher);
CREATE INDEX idx_sources_issn_l ON sources(issn_l);
CREATE INDEX idx_sources_issn_gin ON sources USING gin(issn);

-- Institutions Tables

CREATE TABLE institutions (
    id text PRIMARY KEY,
    ror text,
    display_name text,
    country_code text,
    type text,
    homepage_url text,
    image_url text,
    image_thumbnail_url text,
    display_name_acronyms jsonb,
    display_name_alternatives jsonb,
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

CREATE TABLE institutions_concepts (
    institution_id text,
    concept_id text,
    score float4,
    PRIMARY KEY (institution_id, concept_id),
    FOREIGN KEY (institution_id) REFERENCES institutions(id)
);

-- Indexes for Institutions
CREATE INDEX idx_institutions_display_name ON institutions(display_name);
CREATE INDEX idx_institutions_country_code ON institutions(country_code);
CREATE INDEX idx_institutions_type ON institutions(type);
CREATE INDEX idx_institutions_ror ON institutions(ror);
CREATE INDEX idx_institutions_name_alternatives_gin ON institutions USING gin(display_name_alternatives);
CREATE INDEX idx_institutions_name_acronyms_gin ON institutions USING gin(display_name_acronyms);

-- OpenAlex Taxonomy Tables (Domains, Fields, Subfields, Topics)

CREATE TABLE domains (
    id text PRIMARY KEY,
    display_name text,
    description text,
    works_count int4,
    cited_by_count int4,
    updated_date timestamp
);

CREATE TABLE fields (
    id text PRIMARY KEY,
    display_name text,
    description text,
    domain_id text,
    works_count int4,
    cited_by_count int4,
    updated_date timestamp,
    FOREIGN KEY (domain_id) REFERENCES domains(id)
);

CREATE TABLE subfields (
    id text PRIMARY KEY,
    display_name text,
    description text,
    field_id text,
    works_count int4,
    cited_by_count int4,
    updated_date timestamp,
    FOREIGN KEY (field_id) REFERENCES fields(id)
);

CREATE TABLE topics (
    id text PRIMARY KEY,
    display_name text,
    description text,
    subfield_id text,
    works_count int4,
    cited_by_count int4,
    updated_date timestamp,
    FOREIGN KEY (subfield_id) REFERENCES subfields(id)
);

-- Counts by Year for Taxonomy Entities
CREATE TABLE domains_counts_by_year (
    domain_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (domain_id, year),
    FOREIGN KEY (domain_id) REFERENCES domains(id)
);

CREATE TABLE fields_counts_by_year (
    field_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (field_id, year),
    FOREIGN KEY (field_id) REFERENCES fields(id)
);

CREATE TABLE subfields_counts_by_year (
    subfield_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (subfield_id, year),
    FOREIGN KEY (subfield_id) REFERENCES subfields(id)
);

CREATE TABLE topics_counts_by_year (
    topic_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (topic_id, year),
    FOREIGN KEY (topic_id) REFERENCES topics(id)
);

-- Indexes for Taxonomy Tables
CREATE INDEX idx_domains_display_name ON domains(display_name);
CREATE INDEX idx_fields_display_name ON fields(display_name);
CREATE INDEX idx_subfields_display_name ON subfields(display_name);
CREATE INDEX idx_topics_display_name ON topics(display_name);

-- Publishers Tables

CREATE TABLE publishers (
    id text PRIMARY KEY,
    display_name text,
    alternate_titles jsonb,
    works_count int4,
    cited_by_count int4,
    ids jsonb,
    image_url text,
    country_codes jsonb,
    parent_publisher text,
    updated_date timestamp
);

CREATE TABLE publishers_ids (
    publisher_id text PRIMARY KEY,
    openalex text,
    ror text,
    wikidata text,
    FOREIGN KEY (publisher_id) REFERENCES publishers(id)
);

CREATE TABLE publishers_counts_by_year (
    publisher_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (publisher_id, year),
    FOREIGN KEY (publisher_id) REFERENCES publishers(id)
);

-- Indexes for Publishers
CREATE INDEX idx_publishers_display_name ON publishers(display_name);
CREATE INDEX idx_publishers_parent_publisher ON publishers(parent_publisher);
CREATE INDEX idx_publishers_alternate_titles_gin ON publishers USING gin(alternate_titles);
CREATE INDEX idx_publishers_country_codes_gin ON publishers USING gin(country_codes);

-- Merged Entities Tracking

CREATE TABLE merged_entities (
    original_id text PRIMARY KEY,
    merged_into_id text NOT NULL,
    entity_type text NOT NULL,
    merge_date date NOT NULL,
    merge_reason text
);

-- Indexes for Merged Entities
CREATE INDEX idx_merged_entities_merged_into_id ON merged_entities(merged_into_id);
CREATE INDEX idx_merged_entities_entity_type ON merged_entities(entity_type);
CREATE INDEX idx_merged_entities_merge_date ON merged_entities(merge_date);

-- Merged Entities Log Table (to track merge history)
CREATE TABLE merged_entities_log (
    log_id serial PRIMARY KEY,
    original_id text NOT NULL,
    merged_into_id text NOT NULL,
    entity_type text NOT NULL,
    merge_date date NOT NULL,
    merge_reason text,
    logged_at timestamp DEFAULT CURRENT_TIMESTAMP
);

-- Function to log merged entities
CREATE OR REPLACE FUNCTION log_merged_entity()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO merged_entities_log (
        original_id, 
        merged_into_id, 
        entity_type, 
        merge_date, 
        merge_reason
    ) VALUES (
        NEW.original_id,
        NEW.merged_into_id,
        NEW.entity_type,
        NEW.merge_date,
        NEW.merge_reason
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to log merged entities
CREATE TRIGGER merged_entities_log_trigger
AFTER INSERT ON merged_entities
FOR EACH ROW
EXECUTE FUNCTION log_merged_entity();
