import os
from datetime import timedelta

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "Mika2u7w")
SESSION_COOKIE_NAME = "superset_session"
PERMANENT_SESSION_LIFETIME = timedelta(hours=12)
SESSION_REFRESH_EACH_REQUEST = True
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False
