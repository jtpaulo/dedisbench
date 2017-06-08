/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#include <stdint.h>

//TODO: should this first call be public?
int initialize_nurand(uint64_t totb);
uint64_t get_ioposition_tpcc(uint64_t totb, uint64_t block_size);
uint64_t get_ioposition_uniform(uint64_t totb, uint64_t block_size);
uint64_t get_ioposition_seq(uint64_t totb,uint64_t cont, uint64_t block_size);
