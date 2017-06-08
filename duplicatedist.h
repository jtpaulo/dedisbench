/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */


#include <stdint.h>
#include "berk.h"

//FIle can be in source directory or in /etc/dedisbench if installed with deb package
#define DFILE "dist_personalfiles"
//TODO change this in a future implementation
#define DFILEA "/etc/dedisbench/"

//print dist file DB
DB **dbpdist; // DB structure handle
DB_ENV **envpdist;
#define DISTDB "benchdbs/distdb/"

/*
//Merge statistic lists DB
DB **dbpstat; // DB structure handle
DB_ENV **envpstat;
#define STATDB "statdb/"
*/


//Number of distinct content blocks with duplicates
uint64_t duplicated_blocks;
//= 1839041;

//TOtal Number of blocks at the data set
uint64_t total_blocks;

//array with data collected from Homer (for each duplicated block, the amount of duplicates)
//for homer 1839041
uint64_t *stats;

//array cumulative value of stats sum[n] = stats[n]+sum[n-1] used for having
//distribution for duplicate generation
uint64_t *sum;
//[1839041];

uint64_t *statistics;

//shared mem
uint64_t *zerodups;


void get_distibution_stats(char* fname);
void load_duplicates(char* fname);
void load_cumulativedist(int distout);
uint64_t search(uint64_t value,int low, int high, uint64_t *res);
uint64_t get_writecontent(uint64_t *contnu);
int gen_outputdist(DB **dbpor,DB_ENV **envpor);
