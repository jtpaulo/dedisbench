/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

//random generation
#include "randomgen/SFMT.c"
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>

//TODO: find a more suitable library in the future to prevent the tweaks we are doing below

void init_rand(uint64_t seed){

init_gen_rand(seed);

}


//generates random number between 0 and max
//TODO: maybe the distribution is not completely uniform due to the
//correction done below
uint64_t genrand(uint64_t max){

	//the library generates between 0 and max uint32_t
    uint64_t t = gen_rand32();

    //this must have equal propability, if t is higher than the last number that
    //int32-max is divisible for with remainig zero.
    //we must calculate a random number again, or it stops having equal probability.
    while(t>=INT64_MAX/max * max){

          t = gen_rand64();

    }

    //this way we only et a random from 0 to max
    return t%max;
}
