EXTENSION    = cyanaudit
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

DOCS         = $(wildcard doc/*.md)
SCRIPTS      = $(wildcard tools/*)

PG_CONFIG    = pg_config
DATA         = $(wildcard sql/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN  = sql/$(EXTENSION)--$(EXTVERSION).sql


PG91         = $(shell $(PG_CONFIG) --version | grep -qE " 8\\.| 9\\.0| 9\\.1\\.[0-6]" && echo no || echo yes)

ifeq ($(PG91),no)
$(error "Cyan Audit requires PostgreSQL 9.1.7 or above")
endif

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@


PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
