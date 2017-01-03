EXTENSION    = cyanaudit
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

DOCS         = $(wildcard doc/*.md)
SCRIPTS      = $(wildcard tools/*.p[lm]) $(wildcard tools/*.sh)

PG_CONFIG    = pg_config
DATA         = $(wildcard sql/$(EXTENSION)--*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

tags:
	ctags -f .tags -h ".pm" -R .


###############################
### Verify required version ###
###############################

PGREQVER     = $(shell $(PG_CONFIG) --version | grep -qE " 8\\.| 9\\.[012345]" && echo no || echo yes)

ifeq ($(PGREQVER),no)
$(error "Cyan Audit requires PostgreSQL 9.6.0 or above")
endif



#############################
### Packaging for release ###
#############################
PKGFILES     = cyanaudit.control LICENSE README.md Makefile META.json $(DATA) $(DOCS) $(SCRIPTS)

PKGNAME      = $(EXTENSION)-$(EXTVERSION)
PKG_TGZ      = dist/$(PKGNAME).tar.gz

# Target to create a tarball of all PKGFILES in cyanaudit-XX.YY.ZZ.tar.gz
sdist: $(PKG_TGZ)


# Tarball must be rebuilt anytime a package file changes
$(PKG_TGZ): $(PKGFILES)
	ln -sf . $(PKGNAME)
	mkdir -p dist
	rm -f $(PKG_TGZ)
	tar zcvf $(PKG_TGZ) $(addprefix $(PKGNAME)/,$^)
	rm $(PKGNAME)

