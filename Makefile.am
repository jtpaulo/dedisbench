AUTOMAKE_OPTIONS = subdir-objects
CXXFLAGS = -Wall -Irandomgen 
bin_PROGRAMS=DEDISbench DEDISgen DEDISgenutils
DEDISbench_SOURCES= random.c berk.c structs.h duplicatedist.c iodist.c io.c populate.c sharedmem.c DEDISbench.c inih/ini.c
DEDISbench_CFLAGS= -Wall -Irandomgen -DINI_INLINE_COMMENT_PREFIXES=\"\#\"
DEDISgen_SOURCES=DEDISgen.c berk.c
DEDISgen_CFLAGS = -Wall
DEDISgenutils_SOURCES=DEDISgen-utils.c berk.c 
DEDISgenutils_CFLAGS = -Wall
