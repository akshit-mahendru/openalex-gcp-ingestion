# scripts/etl/utils/entity_processors.py
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

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

    @classmethod
    def _normalize_oa_status(cls, status: Optional[str]) -> Optional[str]:
        """
        Normalize Open Access status to match database constraints.
        
        :param status: Original OA status
        :return: Normalized status or None
        """
        if not status:
            return None
        
        # Mapping for normalization
        oa_status_mapping = {
            'diamond': 'diamond',  # Keep diamond
            'gold': 'gold',
            'green': 'green',
            'bronze': 'bronze',
            'hybrid': 'hybrid',
            'closed': 'closed'
        }
        
        # Convert to lowercase and get mapped status
        normalized = oa_status_mapping.get(str(status).lower())
        
        # Log warning if status is not recognized
        if not normalized:
            logging.warning(f"Unrecognized OA status: {status}. Using 'closed' as default.")
            normalized = 'closed'
        
        return normalized

    # Keep all existing methods, but replace process_works with this updated version:

    @classmethod
    def process_works(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a work entity with comprehensive field extraction.

        :param data: Raw work data dictionary
        :return: Processed work data or None
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
                'abstract_inverted_index': json.dumps(data.get('abstract_inverted_index', {})) if data.get('abstract_inverted_index') else None,
                'indexed_in': json.dumps(data.get('indexed_in', [])),
                'has_fulltext': data.get('has_fulltext', False),
                'authors_count': data.get('authors_count'),
                'concepts_count': data.get('concepts_count'),
                'topics_count': data.get('topics_count'),
                'referenced_works': json.dumps(data.get('referenced_works', [])),
                'related_works': json.dumps(data.get('related_works', [])),
                'primary_location': json.dumps(data.get('primary_location')) if data.get('primary_location') else None,
                'best_oa_location': json.dumps(data.get('best_oa_location')) if data.get('best_oa_location') else None,
                'locations_count': data.get('locations_count'),
                'primary_topic': json.dumps(data.get('primary_topic')) if data.get('primary_topic') else None,
                'sustainable_development_goals': json.dumps(data.get('sustainable_development_goals', [])),
                'keywords': json.dumps(data.get('keywords', [])),
                'fwci': data.get('fwci'),
                'citation_normalized_percentile': json.dumps(data.get('citation_normalized_percentile')),
                'grants': json.dumps(data.get('grants', [])),
                'apc_list': data.get('apc_list'),
                'apc_paid': data.get('apc_paid'),
                'created_date': data.get('created_date'),
                'updated_date': data.get('updated_date') or data.get('updated'),
                'is_retracted': data.get('is_retracted', False),
                'is_paratext': data.get('is_paratext', False)
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

            # Process Authorships
            works_authorships = []
            for authorship in data.get('authorships', []):
                author = authorship.get('author', {})
                authorship_entry = {
                    'work_id': data.get('id'),
                    'author_id': author.get('id'),
                    'author_position': authorship.get('author_position'),
                    'raw_author_name': authorship.get('raw_author_name'),
                    'raw_affiliation_string': authorship.get('raw_affiliation_strings', [None])[0],
                    'institution_id': None,  # Will be populated from institutions list if available
                    'is_corresponding': authorship.get('is_corresponding', False),
                    'countries': json.dumps(authorship.get('countries', [])),
                    'country_ids': json.dumps(authorship.get('country_ids', []))
                }
                
                # Get first institution if available
                institutions = authorship.get('institutions', [])
                if institutions:
                    authorship_entry['institution_id'] = institutions[0].get('id')
                
                works_authorships.append(authorship_entry)

            # Process Topics
            works_topics = []
            for topic in data.get('topics', []):
                topic_entry = {
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
                }
                works_topics.append(topic_entry)

            # Process Concepts
            works_concepts = [
                {
                    'work_id': data.get('id'),
                    'concept_id': concept.get('id'),
                    'score': concept.get('score'),
                    'display_name': concept.get('display_name'),
                    'level': concept.get('level'),
                    'wikidata': concept.get('wikidata')
                }
                for concept in data.get('concepts', [])
            ]

            # Process Mesh
            works_mesh = [
                {
                    'work_id': data.get('id'),
                    'descriptor_ui': mesh.get('descriptor_ui'),
                    'descriptor_name': mesh.get('descriptor_name'),
                    'qualifier_ui': mesh.get('qualifier_ui'),
                    'qualifier_name': mesh.get('qualifier_name'),
                    'is_major_topic': mesh.get('is_major_topic', False)
                }
                for mesh in data.get('mesh', [])
            ]

            # Process Biblio
            biblio = data.get('biblio', {})
            works_biblio = {
                'work_id': data.get('id'),
                'volume': biblio.get('volume'),
                'issue': biblio.get('issue'),
                'first_page': biblio.get('first_page'),
                'last_page': biblio.get('last_page')
            }

            # Process Counts by Year
            works_counts_by_year = [
                {
                    'work_id': data.get('id'),
                    'year': count.get('year'),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

            return {
                'works': work_data,
                'works_ids': works_ids,
                'works_open_access': works_open_access,
                'works_authorships': works_authorships,
                'works_topics': works_topics,
                'works_concepts': works_concepts,
                'works_mesh': works_mesh,
                'works_biblio': works_biblio,
                'works_counts_by_year': works_counts_by_year
            }
        except Exception as e:
            logging.error(f"Error processing work: {str(e)}")
            logging.error(f"Problematic data: {json.dumps(data, indent=2)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_authors(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process an author entity with comprehensive field extraction.

        :param data: Raw author data dictionary
        :return: Processed author data or None
        """
        try:
            # Main author data
            author_data = {
                'id': data.get('id'),
                'orcid': data.get('orcid'),
                'display_name': data.get('display_name'),
                'display_name_alternatives': json.dumps(data.get('display_name_alternatives', [])) if data.get('display_name_alternatives') else None,
                'works_count': data.get('works_count'),
                'cited_by_count': data.get('cited_by_count'),
                'last_known_institution': data.get('last_known_institution', {}).get('id'),
                'works_api_url': data.get('works_api_url'),
                'updated_date': datetime.utcnow(),
                'created_date': datetime.utcnow()
            }

            # Author IDs
            authors_ids = {
                'author_id': data.get('id'),
                'openalex': data.get('id'),
                'orcid': data.get('orcid'),
                'scopus': data.get('ids', {}).get('scopus'),
                'twitter': data.get('ids', {}).get('twitter'),
                'wikipedia': data.get('ids', {}).get('wikipedia'),
                'mag': data.get('ids', {}).get('mag')
            }

            # Counts by Year
            authors_counts_by_year = [
                {
                    'author_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

            # Concepts
            authors_concepts = [
                {
                    'author_id': data.get('id'),
                    'concept_id': concept.get('id'),
                    'score': concept.get('score')
                }
                for concept in data.get('x_concepts', [])
            ]

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

        :param data: Raw source data dictionary
        :return: Processed source data or None
        """
        try:
            # Main source data
            source_data = {
                'id': data.get('id'),
                'issn_l': data.get('issn_l'),
                'issn': json.dumps(data.get('issn', [])) if data.get('issn') else None,
                'display_name': data.get('display_name'),
                'publisher': data.get('publisher'),
                'works_count': data.get('works_count'),
                'cited_by_count': data.get('cited_by_count'),
                'is_in_doaj': data.get('is_in_doaj', False),
                'is_oa': data.get('is_oa', False),
                'homepage_url': data.get('homepage_url'),
                'works_api_url': data.get('works_api_url'),
                'updated_date': datetime.utcnow()
            }

            # Sources IDs
            sources_ids = {
                'source_id': data.get('id'),
                'openalex': data.get('id'),
                'issn_l': data.get('issn_l'),
                'issn': json.dumps(data.get('issn', [])) if data.get('issn') else None,
                'mag': data.get('ids', {}).get('mag')
            }

            # Counts by Year
            sources_counts_by_year = [
                {
                    'source_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

            # Concepts
            sources_concepts = [
                {
                    'source_id': data.get('id'),
                    'concept_id': concept.get('id'),
                    'score': concept.get('score')
                }
                for concept in data.get('x_concepts', [])
            ]

            return {
                'sources': source_data,
                'sources_ids': sources_ids,
                'sources_counts_by_year': sources_counts_by_year,
                'sources_concepts': sources_concepts
            }
        except Exception as e:
            logging.error(f"Error processing source: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_institutions(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process an institution entity with comprehensive field extraction.

        :param data: Raw institution data dictionary
        :return: Processed institution data or None
        """
        try:
            # Main institution data
            institution_data = {
                'id': data.get('id'),
                'ror': data.get('ror'),
                'display_name': data.get('display_name'),
                'country_code': data.get('country_code'),
                'type': data.get('type'),
                'homepage_url': data.get('homepage_url'),
                'image_url': data.get('image_url'),
                'image_thumbnail_url': data.get('image_thumbnail_url'),
                'display_name_acronyms': json.dumps(data.get('display_name_acronyms', [])) if data.get('display_name_acronyms') else None,
                'display_name_alternatives': json.dumps(data.get('display_name_alternatives', [])) if data.get('display_name_alternatives') else None,
                'works_count': data.get('works_count'),
                'cited_by_count': data.get('cited_by_count'),
                'works_api_url': data.get('works_api_url'),
                'updated_date': datetime.utcnow()
            }

            # Institution IDs
            institutions_ids = {
                'institution_id': data.get('id'),
                'openalex': data.get('id'),
                'ror': data.get('ror'),
                'grid': data.get('ids', {}).get('grid'),
                'wikipedia': data.get('ids', {}).get('wikipedia'),
                'wikidata': data.get('ids', {}).get('wikidata'),
                'mag': data.get('ids', {}).get('mag')
            }

            # Geographical Information
            geo_data = data.get('geo', {})
            institutions_geo = {
                'institution_id': data.get('id'),
                'city': geo_data.get('city'),
                'geonames_city_id': geo_data.get('geonames_city_id'),
                'region': geo_data.get('region'),
                'country': geo_data.get('country'),
                'latitude': geo_data.get('latitude'),
                'longitude': geo_data.get('longitude')
            }

            # Counts by Year
            institutions_counts_by_year = [
                {
                    'institution_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

            # Associated Institutions
            institutions_associated_institutions = [
                {
                    'institution_id': data.get('id'),
                    'associated_institution_id': assoc.get('id'),
                    'relationship': assoc.get('relationship')
                }
                for assoc in data.get('associated_institutions', [])
            ]

            # Concepts
            institutions_concepts = [
                {
                    'institution_id': data.get('id'),
                    'concept_id': concept.get('id'),
                    'score': concept.get('score')
                }
                for concept in data.get('x_concepts', [])
            ]

            return {
                'institutions': institution_data,
                'institutions_ids': institutions_ids,
                'institutions_geo': institutions_geo,
                'institutions_counts_by_year': institutions_counts_by_year,
                'institutions_associated_institutions': institutions_associated_institutions,
                'institutions_concepts': institutions_concepts
            }
        except Exception as e:
            logging.error(f"Error processing institution: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_domains(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a domain entity.

        :param data: Raw domain data dictionary
        :return: Processed domain data or None
        """
        try:
            # Main domain data
            domain_data = {
                'id': data.get('id'),
                'display_name': data.get('display_name'),
                'description': data.get('description'),
                'works_count': data.get('works_count'),
                'cited_by_count': data.get('cited_by_count'),
                'updated_date': datetime.utcnow()
            }

            # Counts by year
            domains_counts_by_year = [
                {
                    'domain_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

            return {
                'domains': domain_data,
                'domains_counts_by_year': domains_counts_by_year
            }
        except Exception as e:
            logging.error(f"Error processing domain: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_fields(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a field entity.

        :param data: Raw field data dictionary
        :return: Processed field data or None
        """
        try:
            # Main field data
            field_data = {
                'id': data.get('id'),
                'display_name': data.get('display_name'),
                'description': data.get('description'),
                'domain_id': data.get('domain', {}).get('id'),
                'works_count': data.get('works_count'),
                'cited_by_count': data.get('cited_by_count'),
                'updated_date': datetime.utcnow()
            }

            # Counts by year
            fields_counts_by_year = [
                {
                    'field_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

            return {
                'fields': field_data,
                'fields_counts_by_year': fields_counts_by_year
            }
        except Exception as e:
            logging.error(f"Error processing field: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_subfields(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a subfield entity.

        :param data: Raw subfield data dictionary
        :return: Processed subfield data or None
        """
        try:
            # Main subfield data
            subfield_data = {
                'id': data.get('id'),
                'display_name': data.get('display_name'),
                'description': data.get('description'),
                'field_id': data.get('field', {}).get('id'),
                'works_count': data.get('works_count'),
                'cited_by_count': data.get('cited_by_count'),
                'updated_date': datetime.utcnow()
            }

            # Counts by year
            subfields_counts_by_year = [
                {
                    'subfield_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

            return {
                'subfields': subfield_data,
                'subfields_counts_by_year': subfields_counts_by_year
            }
        except Exception as e:
            logging.error(f"Error processing subfield: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_topics(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a topic entity.

        :param data: Raw topic data dictionary
        :return: Processed topic data or None
        """
        try:
            # Main topic data
            topic_data = {
                'id': data.get('id'),
                'display_name': data.get('display_name'),
                'description': data.get('description'),
                'subfield_id': data.get('subfield', {}).get('id'),
                'works_count': data.get('works_count'),
                'cited_by_count': data.get('cited_by_count'),
                'updated_date': datetime.utcnow()
            }

            # Counts by year
            topics_counts_by_year = [
                {
                    'topic_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

            return {
                'topics': topic_data,
                'topics_counts_by_year': topics_counts_by_year
            }
        except Exception as e:
            logging.error(f"Error processing topic: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    @classmethod
    def process_publishers(cls, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a publisher entity.

        :param data: Raw publisher data dictionary
        :return: Processed publisher data or None
        """
        try:
            # Main publisher data
            publisher_data = {
                'id': data.get('id'),
                'display_name': data.get('display_name'),
                'alternate_titles': json.dumps(data.get('alternate_titles', [])) if data.get('alternate_titles') else None,
                'works_count': data.get('works_count'),
                'cited_by_count': data.get('cited_by_count'),
                'ids': json.dumps(data.get('ids', {})),
                'image_url': data.get('image_url'),
                'country_codes': json.dumps(data.get('country_codes', [])) if data.get('country_codes') else None,
                'parent_publisher': data.get('parent_publisher'),
                'updated_date': datetime.utcnow()
            }

            # Publishers IDs
            publishers_ids = {
                'publisher_id': data.get('id'),
                'openalex': data.get('id'),
                'ror': data.get('ids', {}).get('ror'),
                'wikidata': data.get('ids', {}).get('wikidata')
            }

            # Counts by year
            publishers_counts_by_year = [
                {
                    'publisher_id': data.get('id'),
                    'year': count.get('year'),
                    'works_count': count.get('works_count', 0),
                    'cited_by_count': count.get('cited_by_count', 0)
                }
                for count in data.get('counts_by_year', [])
            ]

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

        :param entity_type: Type of entity to process
        :return: Processor method or None
        """
        processors = {
            'works': cls.process_works,
            'authors': cls.process_authors,
            'sources': cls.process_sources,
            'institutions': cls.process_institutions,
            'domains': cls.process_domains,
            'fields': cls.process_fields,
            'subfields': cls.process_subfields,
            'topics': cls.process_topics,
            'publishers': cls.process_publishers
        }
        return processors.get(entity_type)
