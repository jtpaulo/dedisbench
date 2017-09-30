AUTOMAKE_OPTIONS = subdir-objects
CXXFLAGS = -Wall -Iutils/random/randomgen 
bin_PROGRAMS=DEDISbench DEDISgen DEDISgenutils
DEDISbench_SOURCES= utils/random/random.c utils/db/berk.c structs/structs.h core/duplicates/duplicatedist.c core/accesses/iodist.c core/io.c populate/populate.c core/sharedmem/sharedmem.c DEDISbench.c parserconf/inih/ini.c
DEDISbench_CFLAGS= -Wall -Iutils/random/randomgen -DINI_INLINE_COMMENT_PREFIXES=\"\#\"
DEDISgen_SOURCES=DEDISgen.c utils/db/berk.c
DEDISgen_CFLAGS = -Wall
DEDISgenutils_SOURCES=DEDISgen-utils.c utils/db/berk.c
DEDISgenutils_CFLAGS = -Wall
