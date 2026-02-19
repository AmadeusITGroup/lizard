GENERIC_TARGETS = [
    # Core event context
    "timestamp",         # ISO8601
    "event_type",        # booking, tkt_issued, payment, refund, etc.
    "source",            # file/source name

    # Identity
    "user_id",
    "account_id",
    "device_id",
    "session_id",
    "request_id",
    "ip",
    "user_agent",

    # Travel/ticketing specifics
    "pnr",               # Passenger Name Record / booking locator
    "tkt_number",        # Ticket number
    "carrier",           # Airline code
    "origin",            # IATA code
    "dest",              # IATA code
    "dep_time",          # Departure time
    "arr_time",          # Arrival time
    "legs",              # JSON array of legs (for multi-leg itineraries)
    "office_id",         # Office issuing the ticket
    "user_sign",         # User sign / agent
    "organization",      # Agency/organization

    # Payment
    "amount",
    "currency",
    "payment_method",
    "card_hash",

    # Geo
    "geo_lat",
    "geo_lon",
    "country",
    "city",
    "region",
    "pos_country",       # Point of sale country
    "issue_country",     # Ticket issue country
    "card_country",      # Card country

    # Stay/travel details
    "advance_hours",     # Hours between booking and departure
    "stay_nights",       # Nights at destination

    # Other business fields
    "order_id",          # For e-commerce
    "refund_id",         # For refunds
    "status",            # Success/failure/cancelled
]