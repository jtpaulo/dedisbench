/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#ifndef FAULT_H
#define FAULT_H

#include "duplicatedist.h"
#include "defines.h"

#define DIST_GEN 0
#define TOP_DUP 1
#define BOT_DUP 2
#define UNIQUE 3

#define TIME_F 1
#define OPS_F 2

#define TP_1 0

int inject_failure(int fault_type, int fault_dist, struct duplicates_info *info, uint64_t block_size);
int fault_split(char* a_str, struct user_confs *conf);
void define_failure_per_process(struct user_confs *conf);
#endif