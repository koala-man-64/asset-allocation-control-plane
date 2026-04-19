import json
import sys
args = sys.argv[1:]
if args[:2] == ["repo", "view"]:
    print(json.dumps({
        "name": "asset-allocation-control-plane",
        "nameWithOwner": "koala-man-64/asset-allocation-control-plane",
        "owner": {"login": "koala-man-64"},
        "defaultBranchRef": {"name": "main"},
    }))
elif args[:2] == ["variable", "list"]:
    print("[]")
else:
    print("[]")
