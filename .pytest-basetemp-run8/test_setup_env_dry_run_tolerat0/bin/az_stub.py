import json
import sys
args = sys.argv[1:]
if args[:2] == ["account", "show"]:
    print(json.dumps({"tenantId": "tenant-id", "id": "subscription-id"}))
elif args[:2] == ["group", "show"]:
    print(json.dumps({"name": "AssetAllocationRG", "location": "eastus"}))
elif args[:2] == ["acr", "list"]:
    print(json.dumps([{"name": "assetallocationacr"}]))
elif args[:2] == ["identity", "list"]:
    print(json.dumps([{"resourceGroup": "ignored"}, {"name": "asset-allocation-acr-pull-mi"}]))
elif args[:3] == ["containerapp", "env", "show"]:
    print(json.dumps({
        "name": "asset-allocation-env",
        "properties": {
            "appLogsConfiguration": {
                "logAnalyticsConfiguration": {"customerId": "workspace-id"}
            }
        },
    }))
elif args[:3] == ["containerapp", "env", "list"]:
    print(json.dumps([{"resourceGroup": "ignored"}, {"name": "asset-allocation-env"}]))
elif args[:2] == ["containerapp", "show"]:
    print(json.dumps({
        "name": "asset-allocation-api",
        "properties": {
            "configuration": {"ingress": {"fqdn": "asset-allocation-api.example.test"}},
            "template": {"containers": [{"env": [{"name": "API_ROOT_PREFIX", "value": "asset-allocation"}]}]},
        },
    }))
elif args[:2] == ["containerapp", "list"]:
    print(json.dumps([{"resourceGroup": "ignored"}, {"name": "asset-allocation-api"}]))
elif args[:4] == ["monitor", "log-analytics", "workspace", "list"]:
    print(json.dumps([{"resourceGroup": "ignored"}, {"name": "asset-allocation-law", "customerId": "workspace-id"}]))
elif args[:3] == ["storage", "account", "list"]:
    print(json.dumps([{"name": "assetallocstorage001"}]))
elif args[:4] == ["postgres", "flexible-server", "db", "list"]:
    print(json.dumps([{"name": "asset_allocation"}]))
elif args[:3] == ["postgres", "flexible-server", "list"]:
    print(json.dumps([{"name": "pg-asset-allocation", "administratorLogin": "assetallocadmin"}]))
elif args[:3] == ["ad", "app", "list"]:
    display_name = args[args.index("--display-name") + 1] if "--display-name" in args else "unknown"
    print(json.dumps([{"id": "missing-display-name"}, {"displayName": display_name, "appId": f"{display_name}-app-id"}]))
else:
    print("[]")
