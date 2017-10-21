/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by M. Freitas
 */

#include "utils.h"

int powr(int base, int exp){
	int result = 1;
	while(exp){
		if(exp & 1)
			result *= base;
		exp /= 2;
		base *= base;
	}
	return result;
}

int order_of_magnitude(unsigned long long int key){
	int bucket = 0;
	while(key){
		key /= 10;
		bucket++;
	}
	return bucket;
}
