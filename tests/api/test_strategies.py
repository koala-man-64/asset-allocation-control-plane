import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from api.service.app import app
from core.universe_repository import UniverseRepository


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

# Mock AuthManager verify_jwt dependency to bypass auth
@pytest.fixture(autouse=True)
def mock_auth():
    with patch("api.endpoints.strategies.validate_auth") as mock:
        yield mock

# Mock data
MOCK_STRATEGY = {
    "name": "test-strategy",
    "type": "configured",
    "description": "Test Description",
    "updated_at": "2023-01-01T00:00:00Z"
}

MOCK_UNIVERSE = {
    "source": "postgres_gold",
    "root": {
        "kind": "group",
        "operator": "and",
        "clauses": [
            {
                "kind": "condition",
                "field": "market.close",
                "operator": "gt",
                "value": 10,
            }
        ],
    },
}

MOCK_CONFIG = {
    "universeConfigName": "large-cap-quality",
    "rebalance": "monthly"
}

MOCK_STRATEGY_DETAIL = {
    "name": "test-strategy",
    "type": "configured",
    "description": "Test Description",
    "updated_at": "2023-01-01T00:00:00Z",
    "config": {
        **MOCK_CONFIG,
        "intrabarConflictPolicy": "stop_first",
        "exits": [],
    },
}

@pytest.fixture
def mock_repo():
    with patch("api.endpoints.strategies.StrategyRepository") as mock:
        yield mock

def test_list_strategies(client, mock_repo):
    # Setup mock
    repo_instance = mock_repo.return_value
    repo_instance.list_strategies.return_value = [MOCK_STRATEGY]
    
    response = client.get("/api/strategies/")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "test-strategy"

def test_list_strategies_empty(client, mock_repo):
    repo_instance = mock_repo.return_value
    repo_instance.list_strategies.return_value = []
    
    response = client.get("/api/strategies/")
    assert response.status_code == 200
    assert len(response.json()) == 0

def test_get_strategy(client, mock_repo):
    repo_instance = mock_repo.return_value
    repo_instance.get_strategy_config.return_value = MOCK_CONFIG
    
    response = client.get("/api/strategies/test-strategy")
    assert response.status_code == 200
    assert response.json()["universeConfigName"] == "large-cap-quality"
    assert response.json()["intrabarConflictPolicy"] == "stop_first"
    assert response.json()["exits"] == []

def test_get_strategy_not_found(client, mock_repo):
    repo_instance = mock_repo.return_value
    repo_instance.get_strategy_config.return_value = None
    
    response = client.get("/api/strategies/non-existent")
    assert response.status_code == 404

def test_get_strategy_detail(client, mock_repo):
    repo_instance = mock_repo.return_value
    repo_instance.get_strategy.return_value = MOCK_STRATEGY_DETAIL

    response = client.get("/api/strategies/test-strategy/detail")
    assert response.status_code == 200
    assert response.json()["name"] == "test-strategy"
    assert response.json()["config"]["universeConfigName"] == "large-cap-quality"
    assert response.json()["config"]["intrabarConflictPolicy"] == "stop_first"

def test_save_strategy(client, mock_repo):
    repo_instance = mock_repo.return_value
    repo_instance.save_strategy.return_value = None

    payload = {
        "name": "new-strategy",
        "config": {
            "universeConfigName": "large-cap-quality",
            "rebalance": "weekly",
            "exits": [{"id": "stop-8", "type": "stop_loss_fixed", "value": 0.08}]
        },
        "description": "New Strategy",
        "type": "configured"
    }

    with patch("api.endpoints.strategies._require_postgres_dsn", return_value="postgresql://test:test@localhost:5432/asset_allocation"), patch.object(
        UniverseRepository,
        "get_universe_config",
        return_value={"name": "large-cap-quality", "config": MOCK_UNIVERSE},
    ):
        try:
            response = client.post("/api/strategies/", json=payload)
        except Exception as e:
            pytest.fail(f"API call failed: {e}")

    assert response.status_code == 200
    repo_instance.save_strategy.assert_called_once()
    saved_config = repo_instance.save_strategy.call_args.kwargs["config"]
    assert saved_config["intrabarConflictPolicy"] == "stop_first"
    assert saved_config["exits"][0]["priority"] == 0
    assert saved_config["exits"][0]["priceField"] == "low"

def test_save_strategy_rejects_duplicate_exit_rule_ids(client, mock_repo):
    repo_instance = mock_repo.return_value
    repo_instance.save_strategy.return_value = None

    payload = {
        "name": "new-strategy",
        "description": "New Strategy",
        "type": "configured",
        "config": {
            "universeConfigName": "large-cap-quality",
            "rebalance": "weekly",
            "exits": [
                {"id": "dup", "type": "stop_loss_fixed", "value": 0.08},
                {"id": "dup", "type": "take_profit_fixed", "value": 0.1},
            ],
        },
    }

    with patch("api.endpoints.strategies._require_postgres_dsn", return_value="postgresql://test:test@localhost:5432/asset_allocation"), patch.object(
        UniverseRepository,
        "get_universe_config",
        return_value={"name": "large-cap-quality", "config": MOCK_UNIVERSE},
    ):
        response = client.post("/api/strategies/", json=payload)

    assert response.status_code == 400
    assert "Duplicate exit rule id 'dup'" in response.json()["detail"]
    repo_instance.save_strategy.assert_not_called()


def test_delete_strategy(client, mock_repo):
    repo_instance = mock_repo.return_value
    repo_instance.delete_strategy.return_value = True

    response = client.delete("/api/strategies/test-strategy")

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    repo_instance.delete_strategy.assert_called_once_with("test-strategy")


def test_delete_strategy_not_found(client, mock_repo):
    repo_instance = mock_repo.return_value
    repo_instance.delete_strategy.return_value = False

    response = client.delete("/api/strategies/non-existent")

    assert response.status_code == 404
    repo_instance.delete_strategy.assert_called_once_with("non-existent")
