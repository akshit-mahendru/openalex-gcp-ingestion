# scripts/etl/utils/entity_processors.py
from typing import Dict, Any

class EntityProcessors:
    @staticmethod
    def process_work(data: Dict[str, Any]) -> Dict[str, Any]:
        """Process work entity."""
        return {
            'id': data.get('id'),
            'doi': data.get('doi'),
            'title': data.get('title'),
            'display_name': data.get('display_name'),
            'publication_year': data.get('publication_year'),
            'publication_date': data.get('publication_date'),
            'type': data.get('type'),
            'cited_by_count': data.get('cited_by_count'),
            'is_retracted': data.get('is_retracted', False),
            'is_paratext': data.get('is_paratext', False),
            'host_venue': data.get('host_venue', {}).get('id') if data.get('host_venue') else None
        }

    @staticmethod
    def process_author(data: Dict[str, Any]) -> Dict[str, Any]:
        """Process author entity."""
        return {
            'id': data.get('id'),
            'orcid': data.get('orcid'),
            'display_name': data.get('display_name'),
            'display_name_alternatives': data.get('display_name_alternatives', []),
            'works_count': data.get('works_count'),
            'cited_by_count': data.get('cited_by_count'),
            'last_known_institution': data.get('last_known_institution', {}).get('id') if data.get('last_known_institution') else None,
            'works_api_url': data.get('works_api_url'),
            'updated_date': data.get('updated_date')
        }

    @staticmethod
    def process_concept(data: Dict[str, Any]) -> Dict[str, Any]:
        """Process concept entity."""
        return {
            'id': data.get('id'),
            'wikidata': data.get('wikidata'),
            'display_name': data.get('display_name'),
            'level': data.get('level'),
            'description': data.get('description'),
            'works_count': data.get('works_count'),
            'cited_by_count': data.get('cited_by_count'),
            'image_url': data.get('image_url'),
            'image_thumbnail_url': data.get('image_thumbnail_url'),
            'works_api_url': data.get('works_api_url'),
            'updated_date': data.get('updated_date')
        }

    @staticmethod
    def get_processor(entity_type: str):
        """Get the appropriate processor for an entity type."""
        processors = {
            'works': EntityProcessors.process_work,
            'authors': EntityProcessors.process_author,
            'concepts': EntityProcessors.process_concept
        }
        return processors.get(entity_type)