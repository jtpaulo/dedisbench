/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by J. Paulo
 */

#ifndef POPULATE_H
#define POPULATE_H

#include <stdint.h>
#include "../structs/defines.h"
#include "../benchcore/duplicates/duplicatedist.h"

#define TMP_FILE "dedisbench_0010test"


//Open rawdevice to write
int open_rawdev(char* rawpath, struct user_confs *conf);

//create the file where the process will perform I/O operations
int create_pfile(int procid, struct user_confs *conf);

//Destroy files created by processes
int destroy_pfile(int procid, struct user_confs *conf);

//populate files/dev
void populate(struct user_confs *conf, struct duplicates_info *info);


void check_integrity(struct user_confs *conf, struct duplicates_info *info);


#endif