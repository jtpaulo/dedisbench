CXXFLAGS = -Wall -Irandomgen
bin_PROGRAMS=DEDISbench DEDISgen
DEDISbench_SOURCES=DEDISbench.c
DEDISbench_CFLAGS= -Wall -Irandomgen -ldb
DEDISgen_SOURCES=DEDISgen.c
DEDISgen_CFLAGS = -Wall -ldb `pkg-config openssl --libs`
