/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by J. Paulo
 */

#ifndef RANDOM_H
#define RANDOM_H

#define MEXP 19937
//random generation
#include <stdint.h>

void init_rand(uint64_t seed);


//generates random number between 0 and max
uint64_t genrand(uint64_t max);

#endif
