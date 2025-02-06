-- Initialize OpenAlex Schema with Full Entity Definitions

-- Create schema and set up extensions
DROP SCHEMA IF EXISTS openalex CASCADE;
CREATE SCHEMA openalex;
SET search_path TO openalex, public;

-- Create required extensions
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Works Table with Comprehensive Schema
CREATE TABLE works (
    id text PRIMARY KEY,
    doi text,
    doi_registration_agency text,
    display_name text,
    title text,
    publication_year int4,
    publication_date text,
    type text,
    type_crossref text,
    type_id text,
    language text,
    language_id text,
    cited_by_count int4,
    cited_by_api_url text,
    abstract_inverted_index jsonb,
    cited_by_percentile_year jsonb,
    is_retracted boolean DEFAULT false,
    is_paratext boolean DEFAULT false,
    indexed_in jsonb,
    has_fulltext boolean DEFAULT false,
    authors_count int4,
    concepts_count int4,
    topics_count int4,
    referenced_works_count int4,
    referenced_works jsonb,
    related_works jsonb,
    primary_location jsonb,
    best_oa_location jsonb,
    locations_count int4,
    primary_topic jsonb,
    sustainable_development_goals jsonb,
    keywords jsonb,
    fwci float8,
    citation_normalized_percentile jsonb,
    grants jsonb,
    apc_list float8,
    apc_paid float8,
    created_date date,
    updated_date timestamp
);

-- Works Detailed Tables
CREATE TABLE works_ids (
    work_id text PRIMARY KEY,
    openalex text,
    doi text,
    mag int8,
    pmid text,
    pmcid text,
    wikidata text,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_locations (
    work_id text,
    source_id text,
    landing_page_url text,
    pdf_url text,
    is_oa boolean,
    version text,
    license text,
    is_accepted boolean,
    is_published boolean,
    PRIMARY KEY (work_id, source_id)
);

CREATE TABLE works_open_access (
    work_id text PRIMARY KEY,
    is_oa boolean,
    oa_status text CHECK (oa_status IN ('gold', 'green', 'bronze', 'hybrid', 'closed', 'diamond')),
    oa_url text,
    any_repository_has_fulltext boolean,
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_authorships (
    work_id text,
    author_position text CHECK (author_position IN ('first', 'middle', 'last')),
    author_id text,
    institution_id text,
    raw_author_name text,
    raw_affiliation_string text,
    is_corresponding boolean DEFAULT false,
    countries jsonb,
    country_ids jsonb,
    PRIMARY KEY (work_id, author_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
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
    display_name text,
    level int2,
    wikidata text,
    PRIMARY KEY (work_id, concept_id)
);

CREATE TABLE works_mesh (
    work_id text,
    descriptor_ui text,
    descriptor_name text,
    qualifier_ui text,
    qualifier_name text,
    is_major_topic boolean,
    PRIMARY KEY (work_id, descriptor_ui, qualifier_ui),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_topics (
    work_id text,
    topic_id text,
    score float4,
    display_name text,
    field_id text,
    field_display_name text,
    subfield_id text,
    subfield_display_name text,
    domain_id text,
    domain_display_name text,
    PRIMARY KEY (work_id, topic_id)
);

CREATE TABLE works_referenced_works (
    work_id text,
    referenced_work_id text,
    PRIMARY KEY (work_id, referenced_work_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_related_works (
    work_id text,
    related_work_id text,
    PRIMARY KEY (work_id, related_work_id),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

CREATE TABLE works_counts_by_year (
    work_id text,
    year int4,
    cited_by_count int4,
    PRIMARY KEY (work_id, year),
    FOREIGN KEY (work_id) REFERENCES works(id)
);

-- Authors Tables
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
    created_date date
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
    display_name text,
    level int2,
    wikidata text,
    PRIMARY KEY (author_id, concept_id),
    FOREIGN KEY (author_id) REFERENCES authors(id)
);

-- Sources (Venues) Tables
CREATE TABLE sources (
    id text PRIMARY KEY,
    display_name text,
    host_organization text,
    host_organization_name text,
    host_organization_lineage jsonb,
    publisher text,
    issn_l text,
    issn jsonb,
    works_count int4,
    cited_by_count int4,
    is_in_doaj boolean,
    is_oa boolean,
    homepage_url text,
    works_api_url text,
    type text,
    type_id text,
    updated_date timestamp
);

CREATE TABLE sources_ids (
    source_id text PRIMARY KEY,
    openalex text,
    issn_l text,
    issn jsonb,
    mag int8,
    wikidata text,
    fatcat text,
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

-- Institutions Tables
CREATE TABLE institutions (
    id text PRIMARY KEY,
    ror text,
    display_name text,
    country_code text,
    type text,
    type_id text,
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

-- Publishers Tables
CREATE TABLE publishers (
    id text PRIMARY KEY,
    display_name text,
    alternate_titles jsonb,
    country_codes jsonb,
    hierarchy_level int4,
    parent_publisher text,
    works_count int4,
    cited_by_count int4,
    sources_count int4,
    homepage_url text,
    image_url text,
    image_thumbnail_url text,
    works_api_url text,
    updated_date timestamp
);

CREATE TABLE publishers_counts_by_year (
    publisher_id text,
    year int4,
    works_count int4,
    cited_by_count int4,
    PRIMARY KEY (publisher_id, year),
    FOREIGN KEY (publisher_id) REFERENCES publishers(id)
);

CREATE TABLE publishers_ids (
    publisher_id text PRIMARY KEY,
    openalex text,
    ror text,
    wikidata text,
    FOREIGN KEY (publisher_id) REFERENCES publishers(id)
);

-- Merged Entities Tracking
CREATE TABLE merged_entities (
    original_id text PRIMARY KEY,
    merged_into_id text NOT NULL,
    entity_type text NOT NULL,
    merge_date date NOT NULL,
    merge_reason text
);

-- Create indexes for better query performance
CREATE INDEX idx_works_doi ON works(doi);
CREATE INDEX idx_works_publication_year ON works(publication_year);
CREATE INDEX idx_works_type ON works(type);
CREATE INDEX idx_works_language ON works(language);
CREATE INDEX idx_works_cited_by_count ON works(cited_by_count);
CREATE INDEX idx_works_authors_count ON works(authors_count);
CREATE INDEX idx_works_title_gin ON works USING gin(to_tsvector('english', title));
CREATE INDEX idx_works_display_name_gin ON works USING gin(to_tsvector('english', display_name));
CREATE INDEX idx_works_abstract_gin ON works USING gin(abstract_inverted_index);

CREATE INDEX idx_authors_display_name ON authors(display_name);
CREATE INDEX idx_authors_orcid ON authors(orcid);
CREATE INDEX idx_authors_last_known_institution ON authors(last_known_institution);
CREATE INDEX idx_authors_cited_by_count ON authors(cited_by_count);
CREATE INDEX idx_authors_works_count ON authors(works_count);

CREATE INDEX idx_sources_display_name ON sources(display_name);
CREATE INDEX idx_sources_publisher ON sources(publisher);
CREATE INDEX idx_sources_issn_l ON sources(issn_l);
CREATE INDEX idx_sources_type ON sources(type);
CREATE INDEX idx_sources_cited_by_count ON sources(cited_by_count);

CREATE INDEX idx_institutions_display_name ON institutions(display_name);
CREATE INDEX idx_institutions_ror ON institutions(ror);
CREATE INDEX idx_institutions_country_code ON institutions(country_code);
CREATE INDEX idx_institutions_type ON institutions(type);

CREATE INDEX idx_publishers_display_name ON publishers(display_name);
CREATE INDEX idx_publishers_parent_publisher ON publishers(parent_publisher);

CREATE INDEX idx_domains_display_name ON domains(display_name);
CREATE INDEX idx_fields_display_name ON fields(display_name);
CREATE INDEX idx_subfields_display_name ON subfields(display_name);
CREATE INDEX idx_topics_display_name ON topics(display_name);

[Previous content remains the same until the last line...]

-- GIN indexes for JSONB and array fields
CREATE INDEX idx_works_referenced_works_gin ON works USING gin(referenced_works);
CREATE INDEX idx_works_related_works_gin ON works USING gin(related_works);
CREATE INDEX idx_works_keywords_gin ON works USING gin(keywords);
CREATE INDEX idx_sources_issn_gin ON sources USING gin(issn);
CREATE INDEX idx_institutions_name_alternatives_gin ON institutions USING gin(display_name_alternatives);
CREATE INDEX idx_institutions_name_acronyms_gin ON institutions USING gin(display_name_acronyms);
CREATE INDEX idx_publishers_alternate_titles_gin ON publishers USING gin(alternate_titles);
CREATE INDEX idx_publishers_country_codes_gin ON publishers USING gin(country_codes);

-- Merged Entities Log Table
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

-- Add comments for major tables
COMMENT ON TABLE works IS 'Main table for academic works and publications';
COMMENT ON TABLE authors IS 'Information about authors of academic works';
COMMENT ON TABLE sources IS 'Publication venues including journals and repositories';
COMMENT ON TABLE institutions IS 'Academic and research institutions';
COMMENT ON TABLE publishers IS 'Publishing organizations';
COMMENT ON TABLE domains IS 'Top-level research domains in the OpenAlex taxonomy';
COMMENT ON TABLE fields IS 'Research fields within domains';
COMMENT ON TABLE subfields IS 'Research subfields within fields';
COMMENT ON TABLE topics IS 'Specific research topics within subfields';
COMMENT ON TABLE merged_entities IS 'Tracking table for merged entity IDs';