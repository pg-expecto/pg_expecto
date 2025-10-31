EXTENSION = pg_expecto      # Stop guessing. Start knowing. pg_expecto : statistical and correlation analysis of PostgreSQL database waitings.
DATA = pg_expecto--3.0.sql  # SQL

# Стандартная часть для использования PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
#make USE_PGXS=1 PG_CONFIG=/postgres/pge/
