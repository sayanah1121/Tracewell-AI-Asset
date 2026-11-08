from ingest_packages import is_ai_name, npm_record, pick_pypi


def test_ai_tokens_match_whole_words_only():
    assert all(map(is_ai_name, ["llm-guard", "mcp_server_fetch", "langchain-openai", "crewai", "my.agents", "llama3-tools", "gpt4"]))
    # 'rag' inside 'storage', 'agent' inside 'reagent', 'lora' inside 'flora' are not AI packages
    assert not any(map(is_ai_name, ["storage-utils", "reagent", "flora", "pragma", "requests"]))


def test_pypi_picks_most_recently_active_ai_projects():
    projects = [{"name": "old-llm", "_last-serial": 10}, {"name": "requests", "_last-serial": 99},
                {"name": "New_MCP.Server", "_last-serial": 50}, {"name": "mid-agent", "_last-serial": 30}]
    # names are PEP 503-normalised, non-AI names dropped, highest serial first, capped at target
    assert pick_pypi(projects, 2) == ["new-mcp-server", "mid-agent"]


def test_npm_search_hit_has_the_fetch_npm_shape():
    hit = {"downloads": {"monthly": 1234}, "package": {
        "name": "@acme/weather-mcp", "version": "1.2.0", "description": "Weather MCP server", "license": "MIT",
        "keywords": ["mcp"], "date": "2026-10-01T00:00:00Z", "maintainers": [{"username": "a"}, {"username": "b"}],
        "links": {"repository": "git+https://github.com/acme/weather-mcp.git"}}}
    r = npm_record(hit)
    assert r["repository"] == {"url": "git+https://github.com/acme/weather-mcp.git"}
    assert (r["version"], r["license"], r["downloads_monthly"], r["maintainers"]) == ("1.2.0", "MIT", 1234, 2)
    assert npm_record({"package": {"name": "x"}})["repository"] is None
