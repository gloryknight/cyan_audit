EXTENSION = auditlog
DATA = auditlog--*.sql cyanaudit--*.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
