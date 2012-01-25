/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>
#include "random.c"

//TODO: this should be a parameter in future
uint64_t block_size;


//The position on the file (block address) to be written is given by TPCC NURrand funcion
//NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
//http://www.tpc.org/tpcc/spec/tpcc_current.pdf

//TODO: these values could be adjusted better for the workload in the future
// x,y range of positions; A varies accordingly to the range size; C is a random
//between 0 and A
uint64_t get_ioposition(uint64_t totb){

  //A=9000
  //TODO: this should vary accordingly to the range size
  uint64_t a = 9000;
  //x is zero TODO: this could also be a parameter...
  uint64_t x = 0;

  //y is the max number of blocks
  uint64_t y = totb-1;

  //C
  uint64_t c = genrand(a+1);

  //Calculate NURand function
  uint64_t res = ((( genrand(a) | genrand(y)) + c) % (y - x + 1)) + x ;

  //OUt of range... this should not happen
  if(res>totb-1)
	  perror("Out of range");

  //res gives a block id and we convert to physical address
  uint64_t resf = res*block_size;

  return resf;
}
