/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */


#ifndef IODIST_H
#define IODIST_H

#include <stdint.h>
#include "../../structs/defines.h"



//TODO: should this first call be public?
int init_ioposition(struct user_confs *conf);
uint64_t get_ioposition(struct user_confs *conf, struct stats *stat, int idproc);
uint64_t get_ioposition_tpcc(uint64_t totb, uint64_t block_size);
uint64_t get_ioposition_uniform(uint64_t totb, uint64_t block_size);
uint64_t get_ioposition_seq(uint64_t totb,uint64_t cont, uint64_t block_size);

#endif
