/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by J. Paulo
 */

#ifndef SHAREDMEM_H
#define SHAREDMEM_H

#include <stdint.h>
#include "../duplicates/duplicatedist.h"
#include "../../structs/defines.h"

int loadmem(struct duplicates_info *info);

int loadmmap(uint64_t **mem,uint64_t *sharedmem_size,int *fd_shared, struct duplicates_info *info, struct user_confs *conf);

int closemmap(uint64_t **mem,uint64_t *sharedmem_size,int *fd_shared);

#endif