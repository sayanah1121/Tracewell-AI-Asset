from resolve_entities import best_edge_per_package, connected_components, edges_declared, edges_fuzzy, edges_shared_repo

CAND = ("candidate_id string, source string, category string, name string, norm_name string, "
        "publisher string, repo string, declared_packages array<string>")
PKG = ("candidate_id string, source string, category string, name string, norm_name string, "
       "publisher string, repo string, declared_packages array<string>, package_key string")


def cands(spark, rows):
    return spark.createDataFrame(rows, CAND)


def pairs(df):
    return sorted((r.src, r.dst) for r in df.collect())


def test_declared_package_links_curated_server_to_its_package(spark):
    df = spark.createDataFrame([
        ("seed:mcp:playwright", "seed", "mcp_server", "Playwright", "playwright", "microsoft",
         "microsoft/playwright-mcp", ["npm:@playwright/mcp"], None),
        ("npm:@playwright/mcp", "npm", "package", "@playwright/mcp", "playwright", "playwright",
         "microsoft/playwright-mcp", None, "npm:@playwright/mcp"),
        ("npm:playwright", "npm", "package", "playwright", "playwright", "microsoft", "microsoft/playwright", None,
         "npm:playwright"),
    ], PKG)
    assert pairs(edges_declared(df)) == [("seed:mcp:playwright", "npm:@playwright/mcp")]


def test_shared_repo_links_one_server_and_its_package(spark):
    df = cands(spark, [
        ("seed:mcp:kubernetes", "seed", "mcp_server", "Kubernetes", "kubernetes", "flux159",
         "flux159/mcp-server-kubernetes", None),
        ("npm:mcp-server-kubernetes", "npm", "package", "mcp-server-kubernetes", "kubernetes", "flux159",
         "flux159/mcp-server-kubernetes", None),
    ])
    assert pairs(edges_shared_repo(df)) == [("seed:mcp:kubernetes", "npm:mcp-server-kubernetes")]


def test_monorepo_guard_blocks_shared_repo(spark):
    # modelcontextprotocol/servers hosts many servers: sharing it must not merge them.
    repo = "modelcontextprotocol/servers"
    df = cands(spark, [
        ("seed:mcp:filesystem", "seed", "mcp_server", "Filesystem", "filesystem", "modelcontextprotocol", repo, None),
        ("seed:mcp:git", "seed", "mcp_server", "Git", "git", "modelcontextprotocol", repo, None),
        ("npm:@modelcontextprotocol/server-filesystem", "npm", "package", "x", "filesystem", "modelcontextprotocol", repo, None),
    ])
    assert pairs(edges_shared_repo(df)) == []


def test_fuzzy_needs_more_than_a_similar_name(spark):
    df = cands(spark, [
        ("seed:mcp:gen-ai", "seed", "mcp_server", "Gen AI", "genai", "picsart", None, None),
        ("npm:@google/genai", "npm", "package", "@google/genai", "genai", "google", "googleapis/js-genai", None),
        ("seed:mcp:weather", "seed", "mcp_server", "Weather", "weather", "acme", None, None),
        ("npm:weather-mcp", "npm", "package", "weather-mcp", "weather", "someone", None, None),
    ])
    # The SDK is not an MCP package and has a different publisher; weather-mcp is an MCP package.
    assert pairs(edges_fuzzy(df)) == [("seed:mcp:weather", "npm:weather-mcp")]


def test_each_package_attaches_to_one_server(spark):
    edges = spark.createDataFrame([
        ("seed:mcp:desktop-commander", "npm:@wonderwhy-er/desktop-commander", "declared_package"),
        ("seed:mcp:commander", "npm:@wonderwhy-er/desktop-commander", "fuzzy_name"),
    ], "src string, dst string, rule string")
    kept = best_edge_per_package(edges)
    assert pairs(kept) == [("seed:mcp:desktop-commander", "npm:@wonderwhy-er/desktop-commander")]

    nodes = spark.createDataFrame([
        ("seed:mcp:desktop-commander", "seed", "mcp_server"),
        ("seed:mcp:commander", "seed", "mcp_server"),
        ("npm:@wonderwhy-er/desktop-commander", "npm", "package"),
    ], "candidate_id string, source string, category string")
    labels = {r.candidate_id: r.label[1:] for r in connected_components(nodes, kept).collect()}
    assert labels["npm:@wonderwhy-er/desktop-commander"] == "seed:mcp:desktop-commander"
    assert labels["seed:mcp:commander"] == "seed:mcp:commander"
