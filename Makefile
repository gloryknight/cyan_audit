EXTENSION    = cyanaudit
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

DOCS         = $(wildcard doc/*.md)
SCRIPTS      = $(wildcard tools/*)

PG_CONFIG    = pg_config
DATA         = $(wildcard sql/*--*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN  = sql/$(EXTENSION)--$(EXTVERSION).sql

PKGFILES     = cyanaudit.control LICENSE Makefile META.json \
		       $(DATA $(DOCS) $(SCRIPTS)

PKGNAME	     = $(EXTENSION)-$(EXTVERSION)
PKG_TGZ	     = dist/$(PKGNAME).tar.gz

PG91         = $(shell $(PG_CONFIG) --version | grep -qE " 8\\.| 9\\.0| 9\\.1\\.[0-6]" && echo no || echo yes)

ifeq ($(PG91),no)
$(error "Cyan Audit requires PostgreSQL 9.1.7 or above")
endif

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

sdist: $(PKGNAME)

$(PKGNAME): $(PKGFILES)
	ln -sf . $(PKGNAME)
	mkdir -p dist
	rm -f $(PKG_TGZ)
	tar zcvf $(PKG_TGZ) $(addprefix $(PKGNAME)/,$^)
	rm $(PKGNAME)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
