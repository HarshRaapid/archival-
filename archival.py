from sqlalchemy import create_engine, text, URL
from sqlalchemy.exc import OperationalError

try:
    # --- 1. Create URL, Engine, and Test Connection ---
    read_url = URL.create(
        drivername="mysql+pymysql",  # <-- The only change is here
        username="uazureuser",
        password= '72pY>2O5^@q3b>6',
        host="prod-coding-platform-service-flexible.mysql.database.azure.com",
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


fetch_edges = text (
    """
SELECT 

"""
)

with engine.connect() as conn:
