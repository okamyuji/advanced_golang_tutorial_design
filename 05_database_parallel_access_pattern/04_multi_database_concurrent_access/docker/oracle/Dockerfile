# Oracle Database 21c XE Dockerfile
FROM container-registry.oracle.com/database/express:21.3.0-xe

# 環境変数設定
ENV ORACLE_PWD=OraclePass123
ENV ORACLE_CHARACTERSET=AL32UTF8

# 初期化スクリプトコピー
COPY init.sql /opt/oracle/scripts/startup/
COPY setup.sql /opt/oracle/scripts/setup/

# ポート公開
EXPOSE 1521
EXPOSE 5500

# ヘルスチェック
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD sqlplus -s system/OraclePass123@localhost:1521/XE <<< "SELECT 1 FROM dual;" || exit 1

# Oracle起動
CMD ["/bin/bash", "-c", "/opt/oracle/runOracle.sh"]
