from mcp_orchestration.dairy_kpi_client import DairyKPIClient

c = DairyKPIClient(api_base_url="http://200.23.18.75:8074/IREGIOService")
df = c._select_kpis(
    ["pct_partos_logrados", "prod_a_305_del_1a_lact", "desecho_vacas_secas_2da_lact"]
)
print(df[["Description", "Code"]])
