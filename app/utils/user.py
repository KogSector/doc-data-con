import uuid

def parse_user_id(user_id_str: str | None) -> uuid.UUID:
    """
    Parses a user_id string into a UUID.
    If the string is already a valid UUID, it returns it.
    If the string is not a valid UUID (e.g., Clerk 'user_xxx' IDs), it generates a deterministic UUID.
    """
    if not user_id_str or user_id_str == "00000000-0000-0000-0000-000000000000":
        return uuid.UUID("00000000-0000-0000-0000-000000000000")
    try:
        return uuid.UUID(user_id_str)
    except ValueError:
        return uuid.uuid5(uuid.NAMESPACE_OID, user_id_str)
