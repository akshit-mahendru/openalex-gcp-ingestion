# scripts/etl/utils/entity_processors.py
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List

class EntityProcessors:
    @staticmethod
    def safe_get(data: Dict[str, Any], key: str, default: Any = None) -> Any:
        """
        Safely get a value from a dictionary.
        
        :param data: Source dictionary
        :param key: Key to retrieve
        :param default: Default value if key is not found
        :return: Retrieved value or default
        """
        try:
            return data.get(key, default)
        except Exception as e:
            logging.error(f"Error getting key {key}: {str(e)}")
            return default
            
    @staticmethod
    def safe_json(value: Any, default: Any = None) -> Optional[str]:
        """
        Safely convert value to JSON string.
        
        :param value: Value to convert
        :param default: Default value if conversion fails
        :return: JSON string or default
        """
        try:
            return json.dumps(value) if value is not None else default
        except Exception as e:
            logging.error(f"Error converting to JSON: {str(e)}")
            return default

    @classmethod
    def _normalize_oa_status(cls, status: Optional[str]) -> Optional[str]:
        """
        Normalize Open Access status to match database constraints.
        
        :param status: Original OA status
        :return: Normalized status or None
        """
        if not status:
            return None
        
        oa_status_mapping = {
            'diamond': 'diamond',
            'gold': 'gold',
            'green': 'green',
            'bronze': 'bronze',
            'hybrid': 'hybrid',
            'closed': 'closed'
        }
        
        normalized = oa_status_mapping.get(str(status).lower())
        if not normalized:
            logging.warning(f"Unrecognized OA status: {status}. Using 'closed' as default.")
            normalized = 'closed'
        
        return normalized

    @classmethod
    def process_works(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a work entity with comprehensive field extraction.
        """
        try:
            # Process primary work details
            work_data = {
                'id': data.get('id'),
                'doi': data.get('doi'),
                'doi_registration_agency': data.get('doi_registration_agency'),
                'title': data.get('title'),
                'display_name': data.get('display_name'),
                'publication_year': data.get('publication_year'),
                'publication_date': data.get('publication_date'),
                'type': data.get('type'),
                'type_crossref': data.get('type_crossref'),
                'type_id': data.get('type_id'),
                'language': data.get('language'),
                'language_id': data.get('language_id'),
                'cited_by_count': data.get('cited_by_count', 0),
                'cited_by_api_url': data.get('cited_by_api_url'),
                'is_retracted': data.get('is_retracted', False),
                'is_paratext': data.get('is_paratext', False),
                'abstract_inverted_index': cls.safe_json(data.get('abstract_inverted_index')),
                'indexed_in': cls.safe_json(data.get('indexed_in', [])),
                'has_fulltext': data.get('has_fulltext', False),
                'authors_count': len(data.get('authorships', [])),
                'concepts_count': len(data.get('concepts', [])),
                'topics_count': len(data.get('topics', [])),
                'referenced_works_count': len(data.get('referenced_works', [])),
                'referenced_works': cls.safe_json(data.get('referenced_works', [])),
                'related_works': cls.safe_json(data.get('related_works', [])),
                'primary_location': cls.safe_json(data.get('primary_location')),
                'best_oa_location': cls.safe_json(data.get('best_oa_location')),
                'locations_count': data.get('locations_count'),
                'primary_topic': cls.safe_json(data.get('primary_topic')),
                'sustainable_development_goals': cls.safe_json(data.get('sustainable_development_goals', [])),
                'keywords': cls.safe_json(data.get('keywords', [])),
                'fwci': data.get('fwci'),
                'citation_normalized_percentile': cls.safe_json(data.get('citation_normalized_percentile')),
                'grants': cls.safe_json(data.get('grants', [])),
                'apc_list': data.get('apc_list'),
                'apc_paid': data.get('apc_paid'),
                'created_date': data.get('created_date'),
                'updated_date': data.get('updated_date') or data.get('updated')
            }

            # Process IDs
            works_ids = {
                'work_id': data.get('id'),
                'openalex': data.get('ids', {}).get('openalex'),
                'doi': data.get('doi'),
                'mag': data.get('ids', {}).get('mag'),
                'pmid': data.get('ids', {}).get('pmid'),
                'pmcid': data.get('ids', {}).get('pmcid'),
                'wikidata': data.get('ids', {}).get('wikidata')
            }

            # Process Open Access
            open_access = data.get('open_access', {})
            works_open_access = {
                'work_id': data.get('id'),
                'is_oa': open_access.get('is_oa', False),
                'oa_status': cls._normalize_oa_status(open_access.get('oa_status')),
                'oa_url': open_access.get('oa_url'),
                'any_repository_has_fulltext': open_access.get('any_repository_has_fulltext', False)
            }

            # Process Locations
            works_locations = []
            locations = data.get('locations', [])
            if not locations and data.get('primary_location'):
                locations = [data['primary_location']]
            
            for location in locations:
                source = location.get('source', {})
                works_locations.append({
                    'work_id': data.get('id'),
                    'source_id': source.get('id'),
                    'landing_page_url': location.get('landing_page_url'),
                    'pdf_url': location.get('pdf_url'),
                    'is_oa': location.get('is_oa', False),
                    'version': location.get('version'),
                    'license': location.get('license'),
                    'is_accepted': location.get('is_accepted', False),
                    'is_published': location.get('is_published', False)
                })

            # Process Authorships
            works_authorships = []
            for authorship in data.get('authorships', []):
                author = authorship.get('author', {})
                institutions = authorship.get('institutions', [])
                
                authorship_entry = {
                    'work_id': data.get('id'),
                    'author_id': author.get('id'),
                    'author_position': authorship.get('author_position'),
                    'raw_author_name': author.get('display_name'),
                    'raw_affiliation_string': next(iter(authorship.get('raw_affiliation_strings', [])), None),
                    'institution_id': institutions[0].get('id') if institutions else None,
                    'is_corresponding': authorship.get('is_corresponding', False),
                    'countries': cls.safe_json(authorship.get('countries', [])),
                    'country_ids': cls.safe_json(authorship.get('country_ids', []))
                }
                works_authorships.append(authorship_entry)

            # Process Topics
            works_topics = []
            for topic in data.get('topics', []):
                works_topics.append({
                    'work_id': data.get('id'),
                    'topic_id': topic.get('id'),
                    'score': topic.get('score'),
                    'display_name': topic.get('display_name'),
                    'field_id': topic.get('field', {}).get('id'),
                    'field_display_name': topic.get('field', {}).get('display_name'),
                    'subfield_id': topic.get('subfield', {}).get('id'),
                    'subfield_display_name': topic.get('subfield', {}).get('display_name'),
                    'domain_id': topic.get('domain', {}).get('id'),
                    'domain_display_name': topic.get('domain', {}).get('display_name')
                })

            # Process Concepts
            works_concepts = []
            for concept in data.get('concepts', []):
                works_concepts.append({
                    'work_id': data.get('id'),
                    'concept_id': concept.get('id'),
                    'score': concept.get('score'),
                    'display_name': concept.get('display_name'),
                    'level': concept.get('level'),
                    'wikidata': concept.get('wikidata')
                })

            # Process Mesh
            works_mesh = []
            for mesh in data.get('mesh', []):
                works_mesh.append({
                    'work_id': data.get('id'),
                    'descriptor_ui': mesh.get('descriptor_ui'),
                    'descriptor_name': mesh.get('descriptor_name'),
                    'qualifier_ui': mesh.get('qualifier_ui'),
                    'qualifier_name': mesh.get('qualifier_name'),
                    'is_major_topic': mesh.get('is_major_topic', False)
                })

            # Process Biblio
            biblio = data.get('biblio', {})
            if biblio:
                works_biblio = {
                    'work_id': data.get('id'),
                    'volume': biblio.get('volume'),
                    'issue': biblio.get('issue'),
                    'first_page': biblio.get('first_page'),
                    'last_page': biblio.get('last_page')
                }
            else:
                works_biblio = None

            # Process Counts by Year
            works_counts_by_year = []
            for count in data.get('counts_by_year', []):
                works_counts_by_year.append({
                    'work_id': data.get('id'),
                    'year': count.get('year'),
                    'cited_by_count': count.get('cited_by_count', 0)
                })

            result = {
                'works': work_data,
                'works_ids': works_ids,
                'works_open_access': works_open_access,
                'works_locations': works_locations,
                'works_authorships': works_authorships,
                'works_topics': works_topics,
                'works_concepts': works_concepts,
                'works_mesh': works_mesh,
                'works_counts_by_year': works_counts_by_year
            }

            if works_biblio:
                result['works_biblio'] = works_biblio

            return result

        except Exception as e:
            logging.error(f"Error processing work: {str(e)}")
            logging.error(f"Problematic data: {json.dumps(data, indent=2)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_authors(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process an author entity with comprehensive field extraction.
        """
        try:
            # Main author data
            author_data = {
                'id': data.get('id'),
                'orcid': data.get('orcid'),
                'display_name': data.get('display_name'),
                'display_name_alternatives': cls.safe_json(data.get('display_name_alternatives', [])),
                'works_count': data.get('works_count', 0),
                'cited_by_count': data.get('cited_by_count', 0),
                'last_known_institution': data.get('last_known_institution', {}).get('id'),
                'works_api_url': data.get('works_api_url'),
                'updated_date': data.get('updated_date') or data.get('updated'),
                'created_date': data.get('created_date')
            }

            # Author IDs
            ids = data.get('ids', {})
            authors_ids = {
                'author_id': data.get('id'),
                'openalex': data.get('id'),
                'orcid': data.get('orcid'),
                'scopus': ids.get('scopus'),
                'twitter': ids.get('twitter'),
                'wikipedia': ids.get('wikipedia'),
                'mag': ids.get('mag')
            }

            # Counts by Year
            authors_counts_by_year = []
            for count in data.get('counts_by_year', []):
                authors_counts_by_year.append({
                    'author_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                })

            # Concepts
            authors_concepts = []
            for concept in data.get('x_concepts', []):
                authors_concepts.append({
                    'author_id': data.get('id'),
                    'concept_id': concept.get('id'),
                    'score': concept.get('score'),
                    'display_name': concept.get('display_name'),
                    'level': concept.get('level'),
                    'wikidata': concept.get('wikidata')
                })

            return {
                'authors': author_data,
                'authors_ids': authors_ids,
                'authors_counts_by_year': authors_counts_by_year,
                'authors_concepts': authors_concepts
            }

        except Exception as e:
            logging.error(f"Error processing author: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_sources(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a source (venue) entity with comprehensive field extraction.
        """
        try:
            # Main source data
            source_data = {
                'id': data.get('id'),
                'display_name': data.get('display_name'),
                'host_organization': data.get('host_organization'),
                'host_organization_name': data.get('host_organization_name'),
                'host_organization_lineage': cls.safe_json(data.get('host_organization_lineage', [])),
                'publisher': data.get('publisher'),
                'issn_l': data.get('issn_l'),
                'issn': cls.safe_json(data.get('issn', [])),
                'works_count': data.get('works_count', 0),
                'cited_by_count': data.get('cited_by_count', 0),
                'is_in_doaj': data.get('is_in_doaj', False),
                'is_oa': data.get('is_oa', False),
                'homepage_url': data.get('homepage_url'),
                'works_api_url': data.get('works_api_url'),
                'type': data.get('type'),
                'type_id': data.get('type_id'),
                'updated_date': data.get('updated_date') or data.get('updated')
            }

            # Sources IDs
            ids = data.get('ids', {})
            sources_ids = {
                'source_id': data.get('id'),
                'openalex': data.get('id'),
                'issn_l': data.get('issn_l'),
                'issn': cls.safe_json(data.get('issn', [])),
                'mag': ids.get('mag'),
                'wikidata': ids.get('wikidata'),
                'fatcat': ids.get('fatcat')
            }

            # Counts by Year
            sources_counts_by_year = []
            for count in data.get('counts_by_year', []):
                sources_counts_by_year.append({
                    'source_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                })

            return {
                'sources': source_data,
                'sources_ids': sources_ids,
                'sources_counts_by_year': sources_counts_by_year
            }

        except Exception as e:
            logging.error(f"Error processing source: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_institutions(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process an institution entity with comprehensive field extraction.
        """
        try:
            # Main institution data
            institution_data = {
                'id': data.get('id'),
                'ror': data.get('ror'),
                'display_name': data.get('display_name'),
                'country_code': data.get('country_code'),
                'type': data.get('type'),
                'type_id': data.get('type_id'),
                'homepage_url': data.get('homepage_url'),
                'image_url': data.get('image_url'),
                'image_thumbnail_url': data.get('image_thumbnail_url'),
                'display_name_acronyms': cls.safe_json(data.get('display_name_acronyms', [])),
                'display_name_alternatives': cls.safe_json(data.get('display_name_alternatives', [])),
                'works_count': data.get('works_count', 0),
                'cited_by_count': data.get('cited_by_count', 0),
                'works_api_url': data.get('works_api_url'),
                'updated_date': data.get('updated_date') or data.get('updated')
            }

            # Institution IDs
            ids = data.get('ids', {})
            institutions_ids = {
                'institution_id': data.get('id'),
                'openalex': data.get('id'),
                'ror': data.get('ror'),
                'grid': ids.get('grid'),
                'wikipedia': ids.get('wikipedia'),
                'wikidata': ids.get('wikidata'),
                'mag': ids.get('mag')
            }

            # Geographical Information
            geo = data.get('geo', {})
            institutions_geo = {
                'institution_id': data.get('id'),
                'city': geo.get('city'),
                'geonames_city_id': geo.get('geonames_city_id'),
                'region': geo.get('region'),
                'country_code': geo.get('country_code'),
                'country': geo.get('country'),
                'latitude': geo.get('latitude'),
                'longitude': geo.get('longitude')
            }

            # Associated Institutions
            institutions_associated = []
            for assoc in data.get('associated_institutions', []):
                institutions_associated.append({
                    'institution_id': data.get('id'),
                    'associated_institution_id': assoc.get('id'),
                    'relationship': assoc.get('relationship')
                })

            # Counts by Year
            institutions_counts_by_year = []
            for count in data.get('counts_by_year', []):
                institutions_counts_by_year.append({
                    'institution_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                })

            return {
                'institutions': institution_data,
                'institutions_ids': institutions_ids,
                'institutions_geo': institutions_geo,
                'institutions_associated_institutions': institutions_associated,
                'institutions_counts_by_year': institutions_counts_by_year
            }

        except Exception as e:
            logging.error(f"Error processing institution: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_publishers(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a publisher entity.
        """
        try:
            # Main publisher data
            publisher_data = {
                'id': data.get('id'),
                'display_name': data.get('display_name'),
                'alternate_titles': cls.safe_json(data.get('alternate_titles', [])),
                'country_codes': cls.safe_json(data.get('country_codes', [])),
                'hierarchy_level': data.get('hierarchy_level'),
                'parent_publisher': data.get('parent_publisher'),
                'works_count': data.get('works_count', 0),
                'cited_by_count': data.get('cited_by_count', 0),
                'sources_count': data.get('sources_count', 0),
                'homepage_url': data.get('homepage_url'),
                'image_url': data.get('image_url'),
                'image_thumbnail_url': data.get('image_thumbnail_url'),
                'works_api_url': data.get('works_api_url'),
                'updated_date': data.get('updated_date') or data.get('updated')
            }

            # Publisher IDs
            ids = data.get('ids', {})
            publishers_ids = {
                'publisher_id': data.get('id'),
                'openalex': data.get('id'),
                'ror': ids.get('ror'),
                'wikidata': ids.get('wikidata')
            }

            # Counts by Year
            publishers_counts_by_year = []
            for count in data.get('counts_by_year', []):
                publishers_counts_by_year.append({
                    'publisher_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                })

            return {
                'publishers': publisher_data,
                'publishers_ids': publishers_ids,
                'publishers_counts_by_year': publishers_counts_by_year
            }

        except Exception as e:
            logging.error(f"Error processing publisher: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def get_processor(cls, entity_type: str):
        """
        Get the appropriate processor for an entity type.
        """
        processors = {
            'works': cls.process_works,
            'authors': cls.process_authors,
            'sources': cls.process_sources,
            'institutions': cls.process_institutions,
            'publishers': cls.process_publishers
        }
        return processors.get(entity_type)