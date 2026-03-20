# mcp

```mermaid
flowchart TB

    User[User / Analyst] --> PreAnalyzer

    subgraph Orchestration
        PreAnalyzer["PreAnalyzer<br/>• KPI batching<br/>• Progress tracking<br/>• MCP invocation"]
    end

    PreAnalyzer --> MCP

    subgraph MCP["MCP – Model Context Protocol"]
        Catalog["KPI Catalog Resolver<br/>(Legacy KPI Map)"]
        Fetcher["KPI Fetcher<br/>(IREGIO API)"]
        Normalizer["Time & Schema Normalizer"]
        Summarizer["Statistical Summarizer<br/>(mean, std, trend)"]

        Catalog --> Fetcher
        Fetcher --> Normalizer
        Normalizer --> Summarizer
    end

    Summarizer --> DomainAgents

    subgraph DomainAgents["Domain-Specific Agents"]
        Fertility["Fertility Agent"]
        Production["Production Agent"]
        Health["Health Agent"]
    end

    Fertility --> Causal
    Production --> Causal
    Health --> Causal

    subgraph Reasoning
        Causal["Causal Risk Engine"]
        LLM["LLM Synthesis Agent"]
    end

    Causal --> LLM
    LLM --> Output["Master Summary"]