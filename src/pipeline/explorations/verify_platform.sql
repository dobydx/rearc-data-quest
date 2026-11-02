-- Scratch pipeline quest_scratch_verify: is ${quest.code_version} substituted in a SQL source?
CREATE OR REFRESH MATERIALIZED VIEW quest_scratch.probe_sql_conf
TBLPROPERTIES ('quest.code_version' = '${quest.code_version}')
AS
SELECT '${quest.code_version}' AS code_version;
