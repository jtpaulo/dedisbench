/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by M. Freitas
 */

#ifndef PLOTIO_H
#define PLOTIO_H

#include "../structs/defines.h"

//
// writes distribution plotfile to be passed to gnuplot
//
void write_plot_file_distribution(FILE*, char*, char*);

//
// write latency/throughput multi plotfile to be passed to gnuplot
//
void write_plot_file_latency_throughput(FILE*, char*, char*,char*);

//
// writes accesses plotfile to be passed to gnuplot
//
void write_plot_file_accesses(FILE*, char*);

//
// processes latency and throughput data a writes it to file
//
int write_latency_throughput_snaps(struct stats*, struct user_confs*, char*);

//
// processes and writes accesses data
//
int write_access_data(const uint64_t* accessesarray, struct user_confs* conf, char* id);

#endif
