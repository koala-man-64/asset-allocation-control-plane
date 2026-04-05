from core import delta_core
from core import config as cfg

# Re-export DeltaTable for use in endpoints
from deltalake import DeltaTable

def get_delta_table(container: str, path: str) -> DeltaTable:
    """
    Dependency to get a DeltaTable instance.
    """
    uri = delta_core.get_delta_table_uri(container, path)
    opts = delta_core.get_delta_storage_options(container)
    return DeltaTable(uri, storage_options=opts)

def resolve_container(layer: str, domain: str = None) -> str:
    """
    Resolves the container name based on the layer and optional domain.
    """
    layer = layer.lower()
    if layer == "silver":
        return cfg.AZURE_CONTAINER_SILVER
    elif layer == "gold":
        if domain:
            return resolve_gold_container(domain)
        # Fallback to market or common if no domain provided, or raise
        # For now, let's assume if it's generic gold request without domain (unlikely in our API), 
        # it might be market. But API always has domain.
        # If domain is None/empty, we can trigger resolve_gold_container with "market" or raise.
        # Given generic "market" is common default:
        return cfg.AZURE_FOLDER_MARKET
    elif layer == "platinum":
        if not cfg.AZURE_CONTAINER_PLATINUM:
            raise ValueError("AZURE_CONTAINER_PLATINUM is not configured.")
        return cfg.AZURE_CONTAINER_PLATINUM
    
    raise ValueError(f"Unknown layer: {layer}")

def resolve_gold_container(domain: str) -> str:
    """
    Resolves Gold container by domain.
    """
    domain = domain.lower()
    if domain == "market":
        return cfg.AZURE_FOLDER_MARKET
    elif domain == "finance":
        return cfg.AZURE_FOLDER_FINANCE
    elif domain == "earnings":
        return cfg.AZURE_FOLDER_EARNINGS
    elif domain == "price-target":
        return cfg.AZURE_FOLDER_TARGETS
    else:
        # Fallback or specific
        return cfg.AZURE_CONTAINER_COMMON
