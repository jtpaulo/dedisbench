#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([FULL-PACKAGE-NAME], [VERSION], [BUG-REPORT-ADDRESS])
AC_CONFIG_SRCDIR([DEDISbench.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_MAKE_SET

AM_INIT_AUTOMAKE
AM_PROG_CC_C_O

# Checks for libraries.

AC_CHECK_LIB([db], [main])
AC_CHECK_LIB([ssl], [main])
AC_CHECK_LIB([crypto], [main])

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h inttypes.h malloc.h stdint.h stdlib.h string.h strings.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_FORK
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_FUNC_STRTOD
AC_CHECK_FUNCS([bzero gettimeofday memset mkdir munmap rmdir strchr strdup strtol])

AC_CONFIG_FILES([Makefile
                 utils/random/randomgen/Makefile])
AC_OUTPUT
