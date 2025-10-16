mysql -h prod-coding-platform-service-flexible.mysql.database.azure.com -u uazureuser -p'#4a5v5^x#6£J}="'

from sqlalchemy import create_engine, text, URL
from sqlalchemy.exc import OperationalError

try:
    # --- 1. Create URL, Engine, and Test Connection ---
    read_url = URL.create(
        drivername="mysql+pymysql",  # <-- The only change is here
        username="azureuser",
        password= 'nP#2vL%qM^eR6z@A',
        host="prod-coding-platform-service-flexible-central-us-backup-test.mysql.database.azure.com",
        database="ra_audit_apigateway",
    )
    
    engine = create_engine(
        read_url, 
        connect_args={"ssl_ca": "/home/ubuntu/Harsh/combined-ca-certificates.pem"}
    )

    with engine.connect() as conn:
        db, user = conn.execute(text("SELECT DATABASE(), USER()")).fetchone()
        print(f"✅ Successfully connected to DB '{db}' as '{user}'.")

except OperationalError as e:
    print("❌ Connection Failed. Please check:")
    print("   - Credentials and database name are correct.")
    print("   - SSL cert 'DigiCertGlobalRootG2.crt.pem' exists in the script's directory.")
    print("   - Your IP is allowed in the Azure firewall.")
    print(f"\n   Details: {e.orig}")


mysql -h prod-coding-platform-service-flexible-central-us-backup-test.mysql.database.azure.com -P 3306 -u azureuser -p'nP#2vL%qM^eR6z@A'



fetch_tables = text ("""
            SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE';
                     """)

fetch_graph = text ("""
                    SELECT
        TABLE_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
    FROM
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE
        REFERENCED_TABLE_SCHEMA = DATABASE()
        AND REFERENCED_TABLE_NAME IS NOT NULL;
                    
                    """)

with engine.connect() as connection:
    tables = pd.read_sql(fetch_tables , connection)

with engine.connect() as connection:
    graph = pd.read_sql(fetch_graph , connection)