"""Bronze table definitions for the wealth-management dataset.

Important: `organisation_id` and `processing_date` are deliberately NOT columns
in the ORC files. They only exist in the folder path
(bronze/<source>/<table>/<organisation_id>/<processing_date>/data.orc) and must
be populated by the Bronze layer. If they come back empty in ClickHouse, the
partition logic is broken.
"""
import pyarrow as pa

MONEY = pa.decimal128(18, 2)
QTY = pa.decimal128(18, 4)
TS = pa.timestamp("us")

ORGS = ["org_8812", "org_4410", "org_9027"]

# table -> (source system, business key, schema)
TABLES = {
    "advisors": {
        "source": "salesforce",
        "business_key": ["advisor_id"],
        "schema": pa.schema([
            ("advisor_id", pa.string()),
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("email", pa.string()),
            ("region", pa.string()),
            ("hire_date", pa.date32()),
            ("status", pa.string()),
            ("updated_at", TS),
        ]),
    },
    "contacts": {
        "source": "salesforce",
        "business_key": ["contact_id"],
        "schema": pa.schema([
            ("contact_id", pa.string()),
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("email", pa.string()),
            ("phone", pa.string()),
            ("city", pa.string()),
            ("risk_profile", pa.string()),
            ("created_at", TS),
            ("updated_at", TS),
        ]),
    },
    "accounts": {
        "source": "salesforce",
        "business_key": ["account_id"],
        "schema": pa.schema([
            ("account_id", pa.string()),
            ("contact_id", pa.string()),
            ("advisor_id", pa.string()),
            ("account_type", pa.string()),
            ("status", pa.string()),
            ("base_currency", pa.string()),
            ("opened_date", pa.date32()),
            ("updated_at", TS),
        ]),
    },
    "securities": {
        "source": "reference_data",
        "business_key": ["security_id"],
        "schema": pa.schema([
            ("security_id", pa.string()),
            ("ticker", pa.string()),
            ("name", pa.string()),
            ("asset_class", pa.string()),
            ("currency", pa.string()),
            ("exchange", pa.string()),
            ("last_price", MONEY),
            ("updated_at", TS),
        ]),
    },
    "fee_schedules": {
        "source": "reference_data",
        "business_key": ["fee_schedule_id"],
        "schema": pa.schema([
            ("fee_schedule_id", pa.string()),
            ("account_id", pa.string()),
            ("fee_type", pa.string()),
            ("rate_bps", pa.int32()),
            ("billing_frequency", pa.string()),
            ("effective_from", pa.date32()),
            ("updated_at", TS),
        ]),
    },
    "portfolios": {
        "source": "blackdiamond",
        "business_key": ["portfolio_id"],
        "schema": pa.schema([
            ("portfolio_id", pa.string()),
            ("account_id", pa.string()),
            ("name", pa.string()),
            ("strategy", pa.string()),
            ("benchmark", pa.string()),
            ("inception_date", pa.date32()),
            ("updated_at", TS),
        ]),
    },
    "holdings": {
        "source": "blackdiamond",
        "business_key": ["holding_id"],
        "schema": pa.schema([
            ("holding_id", pa.string()),
            ("portfolio_id", pa.string()),
            ("security_id", pa.string()),
            ("quantity", QTY),
            ("cost_basis", MONEY),
            ("market_value", MONEY),
            ("as_of_date", pa.date32()),
            ("updated_at", TS),
        ]),
    },
    "transactions": {
        "source": "blackdiamond",
        "business_key": ["transaction_id"],
        "schema": pa.schema([
            ("transaction_id", pa.string()),
            ("account_id", pa.string()),
            ("transaction_type", pa.string()),
            ("amount", MONEY),
            ("currency", pa.string()),
            ("trade_date", pa.date32()),
            ("settle_date", pa.date32()),
            ("description", pa.string()),
            ("created_at", TS),
        ]),
    },
    "trades": {
        "source": "blackdiamond",
        "business_key": ["trade_id"],
        "schema": pa.schema([
            ("trade_id", pa.string()),
            ("portfolio_id", pa.string()),
            ("security_id", pa.string()),
            ("side", pa.string()),
            ("quantity", QTY),
            ("price", MONEY),
            ("executed_at", TS),
            ("status", pa.string()),
        ]),
    },
    "statements": {
        "source": "blackdiamond",
        "business_key": ["statement_id"],
        "schema": pa.schema([
            ("statement_id", pa.string()),
            ("account_id", pa.string()),
            ("period_start", pa.date32()),
            ("period_end", pa.date32()),
            ("opening_balance", MONEY),
            ("closing_balance", MONEY),
            ("generated_at", TS),
        ]),
    },
}
