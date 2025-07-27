FROM mysql:8.0

# 環境変数設定
ENV MYSQL_ROOT_PASSWORD=mysql
ENV MYSQL_DATABASE=testdb
ENV MYSQL_USER=appuser
ENV MYSQL_PASSWORD=apppass

# MySQLカスタム設定をコピー
COPY my.cnf /etc/mysql/conf.d/custom.cnf

# 初期化スクリプトをコピー
COPY init-mysql.sql /docker-entrypoint-initdb.d/

# 追加設定ファイル用ディレクトリ作成
RUN mkdir -p /var/lib/mysql-files

# 権限設定
RUN chown -R mysql:mysql /var/lib/mysql /var/lib/mysql-files

# ヘルスチェック設定
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD mysqladmin ping -h localhost -u root -p$MYSQL_ROOT_PASSWORD || exit 1

# ポート公開
EXPOSE 3306

# ログディレクトリ作成
RUN mkdir -p /var/log/mysql && chown mysql:mysql /var/log/mysql

# エントリーポイント設定
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["mysqld"]
