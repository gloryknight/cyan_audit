EXTENSION    = cyanaudit
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

DOCS         = $(wildcard doc/*.md)
SCRIPTS      = $(wildcard tools/*)

PG_CONFIG    = pg_config
DATA         = $(wildcard sql/$(EXTENSION)--*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
PKG_SQL      = $(wildcard sql/$(EXTENSION)--*--*.sql)

PKGFILES     = cyanaudit.control LICENSE README.md Makefile META.json \
               $(PKG_SQL) $(DOCS) $(SCRIPTS)

PKGNAME      = $(EXTENSION)-$(EXTVERSION)
PKG_TGZ      = dist/$(PKGNAME).tar.gz

PG91         = $(shell $(PG_CONFIG) --version | grep -qE " 8\\.| 9\\.0| 9\\.1\\.[0-6]" && echo no || echo yes)

ifeq ($(PG91),no)
$(error "Cyan Audit requires PostgreSQL 9.1.7 or above")
endif

sdist: $(PKGNAME)

$(PKGNAME): $(PKGFILES)
	ln -sf . $(PKGNAME)
	mkdir -p dist
	rm -f $(PKG_TGZ)
	tar zcvf $(PKG_TGZ) $(addprefix $(PKGNAME)/,$^)
	rm $(PKGNAME)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
