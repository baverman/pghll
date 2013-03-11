MODULES = pghll
EXTENSION = pghll
DATA = pghll--1.0.sql
#DOCS = README.pghll

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
