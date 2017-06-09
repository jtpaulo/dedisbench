/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#ifndef POPULATE_H
#define POPULATE_H

#include <stdint.h>
#include "defines.h"

//Open rawdevice to write
int open_rawdev(char* rawpath, struct user_confs *conf);

//create the file where the process will perform I/O operations
int create_pfile(int procid, int odirectf, struct user_confs *conf);

//Destroy files created by processes
int destroy_pfile(int procid, struct user_confs *conf);

//populate process files with content
void populate_pfiles(struct user_confs* conf);


#endif