# ============================================
# GEO SERVICE - CÁLCULO DE DISTÂNCIA
# ============================================

import math
from typing import Optional, Tuple

from src.config import settings


class GeoService:
    """
    Serviço de cálculos geoespaciais.
    
    Implementa fórmula de Haversine para distância entre coordenadas.
    """
    
    @staticmethod
    def haversine(
        lat1: float,
        lng1: float,
        lat2: float,
        lng2: float
    ) -> Optional[float]:
        """
        Calcula distância em km entre dois pontos (lat, lng).
        
        Args:
            lat1, lng1: Coordenadas do ponto A (ex: restaurante)
            lat2, lng2: Coordenadas do ponto B (ex: cliente)
        
        Returns:
            Distância em km (arredondada 2 casas) ou None se inválido
        """
        # Validar coordenadas
        if not all(isinstance(x, (int, float)) for x in [lat1, lng1, lat2, lng2]):
            return None
        
        if not (-90 <= lat1 <= 90 and -90 <= lat2 <= 90):
            return None
        
        if not (-180 <= lng1 <= 180 and -180 <= lng2 <= 180):
            return None
        
        # Raio da Terra em km
        R = settings.earth_radius_km
        
        # Converter para radianos
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lng = math.radians(lng2 - lng1)
        
        # Fórmula de Haversine
        a = (math.sin(delta_lat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * 
             math.sin(delta_lng / 2) ** 2)
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        distance = R * c
        
        return round(distance, 2)
    
    @staticmethod
    def classify_distance_zone(
        distance_km: Optional[float],
        threshold_near: float = 2.0,
        threshold_medium: float = 5.0
    ) -> Optional[str]:
        """
        Classifica distância em zona: near, medium, far.
        
        Args:
            distance_km: Distância em km
            threshold_near: Limite para 'near' (padrão: 2km)
            threshold_medium: Limite para 'medium' (padrão: 5km)
        
        Returns:
            'near', 'medium', 'far', ou None se distance_km for None
        """
        if distance_km is None:
            return None
        
        if distance_km <= threshold_near:
            return 'near'
        elif distance_km <= threshold_medium:
            return 'medium'
        else:
            return 'far'
    
    @staticmethod
    def extract_coordinates_from_address(address: dict) -> Tuple[Optional[float], Optional[float]]:
        """
        Extrai lat/lng de objeto de endereço do Cardapioweb.
        
        Args:
            address: Dict com possíveis chaves 'lat', 'lng', 'latitude', 'longitude'
        
        Returns:
            (lat, lng) ou (None, None) se não encontrado
        """
        if not isinstance(address, dict):
            return None, None
        
        # Tentar várias convenções de nomenclatura
        lat = address.get('lat') or address.get('latitude') or address.get('latitud')
        lng = address.get('lng') or address.get('longitude') or address.get('longitud') or address.get('lon')
        
        try:
            return float(lat) if lat is not None else None, float(lng) if lng is not None else None
        except (ValueError, TypeError):
            return None, None